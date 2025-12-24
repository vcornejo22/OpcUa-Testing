from scapy.all import *
from scapy.layers.inet import IP, TCP
from scapy.layers.l2 import Ether


def capturar_paquete(packet):
    # Procesar capa Ethernet
    if packet.haslayer(Ether):
        eth_layer = packet[Ether]
        print("\n---[ ETH ]---")
        print(f"FT_ETHER dst          = {eth_layer.dst}")
        print(f"FT_ETHER src          = {eth_layer.src}")
        print(f"FT_HEX type           = {hex(eth_layer.type)}")

    # Procesar capa IP
    if packet.haslayer(IP):
        ip_layer = packet[IP]
        print("\n---[ IP ]---")
        print(f"FT_HEX version        = {hex(ip_layer.version)}")
        print(f"FT_INT_BE len         = {ip_layer.len}")
        print(f"FT_HEX id             = {hex(ip_layer.id)}")
        print(f"FT_IPv4 src           = {ip_layer.src}")
        print(f"FT_IPv4 dst           = {ip_layer.dst}")

    # Procesar capa TCP
    if packet.haslayer(TCP):
        tcp_layer = packet[TCP]
        print("\n---[ TCP ]---")
        print(f"FT_INT_BE srcport     = {tcp_layer.sport}")
        print(f"FT_INT_BE dstport     = {tcp_layer.dport}")
        print(f"FT_HEX seq            = {hex(tcp_layer.seq)}")
        print(f"FT_HEX ack            = {hex(tcp_layer.ack)}")
        print(f"FT_INT_BE window_size = {tcp_layer.window}")

    # Procesar capa OPC UA (opcional, requiere implementación específica)
    if packet.haslayer(Raw):
        opcua_data = packet[Raw].load
        print("\n---[ OPC UA ]---")
        # Procesar el contenido crudo de OPC UA según el protocolo
        print(f"FT_BYTES payload      = {opcua_data.hex()}")

# Captura paquetes en la interfaz de red en tiempo real
print("Capturando paquetes OPC UA...")
sniff(iface="en0", filter="tcp port 53530", prn=capturar_paquete)
