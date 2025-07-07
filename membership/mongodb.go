package membership

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func generateMemberID() string {
	if hostname := os.Getenv("HOSTNAME"); hostname != "" {
		return hostname
	}

	if podName := os.Getenv("POD_NAME"); podName != "" {
		return podName
	}

	if podIP := os.Getenv("POD_IP"); podIP != "" {
		return fmt.Sprintf("pod-%s-%d", strings.Replace(podIP, ".", "-", -1), time.Now().Unix())
	}

	if localIP := getLocalIP(); localIP != "" {
		return fmt.Sprintf("ip-%s-%d", strings.Replace(localIP, ".", "-", -1), time.Now().Unix())
	}

	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return fmt.Sprintf("%s-%d", hostname, time.Now().Unix())
	}

	return fmt.Sprintf("member-%s", generateShortUUID())
}

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func generateShortUUID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", bytes)
}
