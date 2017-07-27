package test_utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// PubDerToPem converts public key from der format to pem.
func PubDerToPem(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// PrivDerToPem converts rsa private key from der to pem.
func PrivDerToPem(priv *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(priv),
		})
}

// GenerateSelfSignedCert genereate self-signed certificate and returns
// public certificate in pem, private key in pem, error.
func GenerateSelfSignedCert(hostname string) ([]byte, []byte, error) {
	pubDer, privDer, err := generateCertificate(hostname, nil, nil, true, 1*time.Hour)
	if err != nil {
		return nil, nil, err
	}
	return PubDerToPem(pubDer), PrivDerToPem(privDer), nil
}

// generateCertificate generates certificate with rsa private key and signed it
// by parent key if it is provided, otherwise the cert will be signed by its
// private key.
func generateCertificate(
	hostname string,
	parent *x509.Certificate,
	parentKey *rsa.PrivateKey,
	lookupIps bool,
	expiration time.Duration) ([]byte, *rsa.PrivateKey, error) {

	notBefore := time.Now()
	notAfter := notBefore.Add(expiration)
	serialNumLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNum, err := rand.Int(rand.Reader, serialNumLimit)
	if err != nil {
		return nil, nil, err
	}

	isCA := parent == nil

	template := x509.Certificate{
		SerialNumber: serialNum,
		Subject: pkix.Name{
			Organization: []string{"Dropbox"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA: isCA,
	}

	if lookupIps {
		ip := net.ParseIP(hostname)
		if ip == nil {
			var ips []net.IP
			if ips, err = net.LookupIP(hostname); err != nil {
				return nil, nil, err
			}
			if len(ips) > 0 {
				ip = ips[0]
			}
		} else {
			names, errLookup := net.LookupAddr(hostname)
			if errLookup == nil && len(names) > 0 {
				hostname = names[0]
			}
		}
		if ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		}
	}
	template.DNSNames = append(template.DNSNames, hostname)

	// generate private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	if isCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parentKey = priv
	} else {
		parent = &template
	}
	// generate certificate
	pubKey := &priv.PublicKey
	derBytes, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		pubKey,
		parentKey)

	if err != nil {
		return nil, nil, err
	}
	return derBytes, priv, nil
}

// generate CA and certificate signed by generated CA.
// Returns CA's certificate, certificate, private key, error
func GenerateCertWithCA(hostname string) ([]byte, []byte, []byte, error) {
	return GenerateCertWithCAPrefs(hostname, true, time.Hour) // lookupIps
}

func GenerateCertWithCAPrefs(
	hostname string, lookupIps bool, expiration time.Duration) ([]byte, []byte, []byte, error) {

	// generate CA first
	caPubDer, caPriv, err := generateCertificate(hostname, nil, nil, lookupIps, expiration)
	if err != nil {
		return nil, nil, nil, err
	}

	caCert, err := x509.ParseCertificate(caPubDer)
	if err != nil {
		return nil, nil, nil, err
	}

	// generate cert signed by CA
	pubDer, privKey, err := generateCertificate(hostname, caCert, caPriv, lookupIps, expiration)
	if err != nil {
		return nil, nil, nil, err
	}

	return PubDerToPem(caPubDer), PubDerToPem(pubDer), PrivDerToPem(privKey), nil
}
