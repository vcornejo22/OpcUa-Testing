import os
import subprocess
from itertools import product

# Step 2: openssl genrsa -out key.pem 2048
# Step 3: openssl req -x509 -days 365 -new -out certificate.pem -key key.pem -config ssl.conf
# 
# Gen basic128rsa15
# openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 365 -out certificate.pem
# 
# Gen Aes128Sha256RsaOaep
# openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 365 -out certificate.pem -sha256

# Gen Aes256Sha256RsaPss
# openssl genpkey -algorithm RSA -out key.pem -pkeyopt rsa_keygen_bits:2048
# openssl req -new -key key.pem -out csr.pem -config config.cnf -sha256
# openssl x509 -req -days 365 -in csr.pem -signkey key.pem -out certificate.pem -extfile config.cnf -extensions req_ext


def modify_ssl_conf(bits, md, config_file_path="./opcua/ssl.conf"):
    # Read the existing configuration
    with open(config_file_path, "r") as file:
        lines = file.readlines()

    # Modify the configuration
    for i, line in enumerate(lines):
        if line.startswith("default_bits"):
            lines[i] = f"default_bits = {bits}\n"
        elif line.startswith("default_md"):
            lines[i] = f"default_md = {md}\n"

    # Write the modified configuration back
    with open(config_file_path, "w") as file:
        file.writelines(lines)

    print(f"ssl.conf modified with default_bits={bits} and default_md={md}")


def create_certificates(rsa_bits, sa):
    # for config in cert_configs:
    folder_name = f"opcua/{rsa_bits}_{sa}"
    os.makedirs(folder_name, exist_ok=True)
    
    # Generate private key
    print("[+] Generate private key")
    private_key_file = os.path.join(folder_name, f"{rsa_bits}_{sa}_private_key.pem")
    subprocess.run(["openssl", "genrsa", "-out", private_key_file, str(rsa_bits)], check=True)
    
    # Create self-signed certificate
    print("[+] Create self-signed certificate")
    cert_file = os.path.join(folder_name, f"{rsa_bits}_{sa}_certificate.pem")
    subprocess.run([
        "openssl", "req", "-x509", "-days", "365", "-new", "-out", cert_file, "-key", private_key_file, "-config", "opcua/ssl.conf"
    ], check=True)
    
    print(f"Certificate for {rsa_bits} created in folder {folder_name}.")

cert_configs = {
        "rsa_bits": [1024, 2048, 3072, 4096],
        "signature_algorithm": ["Sha1", "Sha224", "Sha256", "Sha384", "Sha512"],
    }
weight = {
    "w_rsa": [1, 2, 3, 4],
    "w_sa": [1, 2, 3, 4, 5]
}

def main():
    # Generate all combinations of rsa_bits and signature_algorithm
    combinations = list(product(cert_configs["rsa_bits"], cert_configs["signature_algorithm"]))
    weight_combinations = list(product(weight["w_rsa"], weight["w_sa"]))
    for combo, w_combo in zip(combinations, weight_combinations):
        modify_ssl_conf(combo[0], combo[1])
        create_certificates(combo[0], combo[1])
        # w = w_combo[0]
        # print(w)
        print(f"rsa_bits: {combo[0]}, signature_algorithm: {combo[1]}, weight:{w_combo[0] * w_combo[1]}")
    # create_certificates(combinations[0][0], combinations[0][1])

if __name__ == "__main__":
    main()


