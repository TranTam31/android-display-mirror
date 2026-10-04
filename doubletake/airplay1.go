package airplay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"time"

	"howett.net/plist"
)

var AirPlay1Mode bool

var ErrAirPlay1PasswordRequired = fmt.Errorf("airplay1 receiver requires password")

func airplay1Request(c *AirPlayClient, method, uri, contentType string, body []byte, extraHeaders map[string]string) ([]byte, map[string]string, error) {
	resp, headers, err := c.rtspRequest(method, uri, contentType, body, extraHeaders)
	var httpErr *HTTPStatusError
	if errors.As(err, &httpErr) && httpErr.StatusCode == 401 {
		return resp, headers, ErrAirPlay1PasswordRequired
	}
	return resp, headers, err
}

var portRegex = regexp.MustCompile(`server_port=(\d+)`)

func parseTransportServerPort(transport string) int {
	match := portRegex.FindStringSubmatch(transport)
	if len(match) > 1 {
		port, _ := strconv.Atoi(match[1])
		return port
	}
	return 0
}

// dataPortFromSetupPlist digs the video data port out of a binary-plist SETUP
// response. HiChip/hccast-class clones (the ones that spoof AppleTV3,2 and show
// "FSCAST") answer the legacy /video SETUP with a plist body instead of a
// Transport header with server_port, so the header regex finds nothing. The
// port can sit in a modern streams[] descriptor or as a flat key depending on
// the firmware, so try both and dump the whole plist for diagnosis.
func dataPortFromSetupPlist(body []byte) int {
	var resp map[string]interface{}
	if _, err := plist.Unmarshal(body, &resp); err != nil {
		dbg("[AIRPLAY1] SETUP body is not a plist (%d bytes): %v", len(body), err)
		return 0
	}
	debugDumpPlist("airplay1 SETUP response", resp)

	if streams, ok := resp["streams"].([]interface{}); ok {
		for _, s := range streams {
			if stream, ok := s.(map[string]interface{}); ok {
				if dp, _ := plistStreamPorts(stream); dp > 0 {
					dbg("[AIRPLAY1] data port from streams[]: %d", dp)
					return dp
				}
			}
		}
	}

	for _, k := range []string{"dataPort", "data_port", "server_port", "serverPort", "port"} {
		if p := plistInt(resp[k]); p > 0 {
			dbg("[AIRPLAY1] data port from top-level %q: %d", k, p)
			return p
		}
	}
	return 0
}

func (c *AirPlayClient) SetupMirrorAirPlay1(ctx context.Context) (*MirrorSession, error) {
	videoURI := fmt.Sprintf("rtsp://%s:%d/video", c.host, c.port)
	setupHeaders := map[string]string{
		"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1;mode=screen",
	}

	respBody, respHeaders, err := airplay1Request(c, "SETUP", videoURI, "", nil, setupHeaders)
	if err != nil {
		return nil, fmt.Errorf("airplay1 SETUP /video: %w", err)
	}
	dataPort := parseTransportServerPort(respHeaders["transport"])
	if dataPort == 0 && len(respBody) > 0 {
		// legacy clones answer with a binary plist instead of a Transport header
		dataPort = dataPortFromSetupPlist(respBody)
	}
	if dataPort == 0 {
		return nil, fmt.Errorf("airplay1 SETUP /video: no server_port in response (headers=%+v, body=%d bytes)", respHeaders, len(respBody))
	}
	dbg("[AIRPLAY1] /video SETUP returned server_port=%d", dataPort)

	if _, _, err := airplay1Request(c, "RECORD", videoURI, "", nil, nil); err != nil {
		return nil, fmt.Errorf("airplay1 RECORD /video: %w", err)
	}

	dataAddr := net.JoinHostPort(c.host, strconv.Itoa(dataPort))
	dataConn, err := net.DialTimeout("tcp", dataAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("airplay1 dial %s: %w", dataAddr, err)
	}
	if tc, ok := dataConn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetWriteBuffer(64 * 1024)
	}
	// data socket stays in HTTP parse mode until the client posts to /stream with a parsable bplist body; without this preamble every subsequent video packet is silently discarded
	preambleBody, err := plist.Marshal(map[string]interface{}{
		"latencyMs": int64(100),
	}, plist.BinaryFormat)
	if err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("airplay1 preamble marshal: %w", err)
	}
	preambleHeader := fmt.Sprintf(
		"POST /stream HTTP/1.1\r\nContent-Type: application/x-apple-binary-plist\r\nContent-Length: %d\r\n\r\n",
		len(preambleBody),
	)
	if _, err := dataConn.Write([]byte(preambleHeader)); err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("airplay1 preamble header: %w", err)
	}
	if _, err := dataConn.Write(preambleBody); err != nil {
		dataConn.Close()
		return nil, fmt.Errorf("airplay1 preamble body: %w", err)
	}
	dbg("[AIRPLAY1] data connected: %s, /stream preamble sent (%d bytes)", dataAddr, len(preambleBody))

	return &MirrorSession{
		client:     c,
		dataConn:   dataConn,
		DataPort:   dataPort,
		sessionURI: videoURI,
		noAudio:    true,
	}, nil
}

func (s *MirrorSession) StreamFramesAirPlay1(ctx context.Context, capture io.Reader) error {
	defer s.Close()
	buf := make([]byte, 64*1024)
	var totalBytes int64
	firstWrite := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := capture.Read(buf)
		if n > 0 {
			if firstWrite {
				dumpN := n
				if dumpN > 32 {
					dumpN = 32
				}
				dbg("[AIRPLAY1] first write: %d bytes, leading hex=%x", n, buf[:dumpN])
				firstWrite = false
			}
			s.dataMu.Lock()
			_, werr := s.dataConn.Write(buf[:n])
			s.dataMu.Unlock()
			if werr != nil {
				return fmt.Errorf("airplay1 data write (after %d bytes total): %w", totalBytes, werr)
			}
			totalBytes += int64(n)
			if totalBytes%(1<<20) < int64(n) {
				dbg("[AIRPLAY1] forwarded %d bytes total", totalBytes)
			}
		}
		if err != nil {
			if err == io.EOF {
				dbg("[AIRPLAY1] capture EOF after %d bytes", totalBytes)
				return nil
			}
			return err
		}
	}
}
