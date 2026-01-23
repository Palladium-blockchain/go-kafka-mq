package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
)

func LoadTLSConfig(caCertPath string) (*tls.Config, error) {
	f, err := os.Open(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CA certificate file %s: %v", caCertPath, err)
	}
	defer func() { _ = f.Close() }()

	fileContent, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate file %s: %v", caCertPath, err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(fileContent) {
		return nil, errors.New("failed to append CA certificate to pool")
	}

	return &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS10,
	}, nil
}
