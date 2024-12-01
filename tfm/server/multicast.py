import socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def send(ip, port, data):
    if isinstance(data, str):
        data = data.encode('utf-8')

    address = (ip, port)
    udp_socket.sendto(data, address)
    #udp_socket.close()
