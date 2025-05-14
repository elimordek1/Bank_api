import subprocess
import os

# Dictionary of company folders and their corresponding .pfx passwords
certificates = {
    "BEST RETAIL GEORGIA LLC": "dA7Lls4D",
    "FASHION RETAIL GEORGIA LLC": "8h5UaLp6",
    "GLOBAL APPAREL GEORGIA LLC": "3OWKl0pc",
    "MASTER HOME RETAIL LLC":   "Ouq8gDMV",
    "MASTER RETAIL GEORGIA LLC": "WXZj2whp",
    "MEGA STORE GEORGIA LLC": "Dsv2d9nv",
    "PRO RETAIL GEORGIA LLC": "YLPF98sJ",
    "RETAIL GROUP GEORGIA LLC": "tB1LM6DJ",
    "RETAIL GROUP HOLDING LLC": "dWBo3ZmU",
    "SPANISH RETAIL GEORGIA LLC": "OGyKno1u",

}

base_cert_dir = "TBC_cert"

for company, password in certificates.items():
    pfx_path = os.path.join(base_cert_dir, company, f"{company}.pfx")
    output_dir = os.path.join(base_cert_dir, company)

    key_pem = os.path.join(output_dir, "key.pem")
    cert_pem = os.path.join(output_dir, "cert.pem")
    key_unencrypted = os.path.join(output_dir, "key_unencrypted.pem")
    server_cert = os.path.join(output_dir, "server_cert.pem")

    if not os.path.exists(pfx_path):
        print(f"[WARNING] PFX file not found for {company}: {pfx_path}")
        continue

    # Extract key.pem
    subprocess.run([
        "openssl", "pkcs12", "-in", pfx_path, "-nocerts",
        "-out", key_pem, "-nodes", "-password", f"pass:{password}"
    ])

    # Extract cert.pem
    subprocess.run([
        "openssl", "pkcs12", "-in", pfx_path, "-nokeys",
        "-out", cert_pem, "-password", f"pass:{password}"
    ])

    # Create key_unencrypted.pem
    subprocess.run([
        "openssl", "rsa", "-in", key_pem,
        "-out", key_unencrypted
    ])

    # Create server_cert.pem
    subprocess.run([
        "openssl", "x509", "-in", cert_pem,
        "-out", server_cert
    ])

    print(f"[INFO] Processed certificate for: {company}")

