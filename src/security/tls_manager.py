"""
TLS/mTLS manager for inter-node encrypted communication.
Auto-generates self-signed CA and node certificates.
"""

import logging
import os
from typing import Optional

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime
import ssl

logger = logging.getLogger(__name__)


class TLSManager:
    """Manages TLS certificates and SSL contexts for secure inter-node communication."""

    def __init__(self, certs_dir: str = "certs", node_id: str = "node-1"):
        self.certs_dir = certs_dir
        self.node_id = node_id
        self.ca_key_path = os.path.join(certs_dir, "ca.key")
        self.ca_cert_path = os.path.join(certs_dir, "ca.crt")
        self.node_key_path = os.path.join(certs_dir, f"{node_id}.key")
        self.node_cert_path = os.path.join(certs_dir, f"{node_id}.crt")

    def setup(self):
        """Generate CA and node certificates if they don't exist."""
        os.makedirs(self.certs_dir, exist_ok=True)
        if not os.path.exists(self.ca_cert_path):
            self._generate_ca()
        if not os.path.exists(self.node_cert_path):
            self._generate_node_cert()
        logger.info(f"[{self.node_id}] TLS certificates ready in {self.certs_dir}")

    def _generate_ca(self):
        """Generate a self-signed CA certificate."""
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DistSync Cluster"),
            x509.NameAttribute(NameOID.COMMON_NAME, "DistSync CA"),
        ])
        cert = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.datetime.utcnow())
                .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(key, hashes.SHA256()))
        self._write_key(self.ca_key_path, key)
        self._write_cert(self.ca_cert_path, cert)
        logger.info("Generated CA certificate")

    def _generate_node_cert(self):
        """Generate a node certificate signed by the CA."""
        ca_key = self._load_key(self.ca_key_path)
        ca_cert = self._load_cert(self.ca_cert_path)
        node_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DistSync Cluster"),
            x509.NameAttribute(NameOID.COMMON_NAME, self.node_id),
        ])
        cert = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(ca_cert.subject)
                .public_key(node_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.datetime.utcnow())
                .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
                .add_extension(
                    x509.SubjectAlternativeName([
                        x509.DNSName(self.node_id),
                        x509.DNSName("localhost"),
                    ]), critical=False)
                .sign(ca_key, hashes.SHA256()))
        self._write_key(self.node_key_path, node_key)
        self._write_cert(self.node_cert_path, cert)
        logger.info(f"Generated certificate for {self.node_id}")

    def create_server_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for the HTTP server (mTLS)."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.node_cert_path, self.node_key_path)
        ctx.load_verify_locations(self.ca_cert_path)
        ctx.verify_mode = ssl.CERT_REQUIRED  # mTLS
        return ctx

    def create_client_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for outgoing connections (mTLS)."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_cert_chain(self.node_cert_path, self.node_key_path)
        ctx.load_verify_locations(self.ca_cert_path)
        ctx.check_hostname = False  # Internal cluster
        return ctx

    def get_cert_info(self) -> dict:
        """Get info about the current node certificate."""
        cert = self._load_cert(self.node_cert_path)
        ca_cert = self._load_cert(self.ca_cert_path)
        return {
            "node_id": self.node_id,
            "subject": str(cert.subject),
            "issuer": str(cert.issuer),
            "not_valid_before": cert.not_valid_before_utc.isoformat(),
            "not_valid_after": cert.not_valid_after_utc.isoformat(),
            "serial_number": str(cert.serial_number),
            "ca_subject": str(ca_cert.subject),
        }

    def _write_key(self, path, key):
        with open(path, "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption()))

    def _write_cert(self, path, cert):
        with open(path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

    def _load_key(self, path):
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def _load_cert(self, path):
        with open(path, "rb") as f:
            return x509.load_pem_x509_certificate(f.read())
