import os
ip_list = [x for x in range(180,182)]
ip_list = [f"172.18.0.{x}" for x in ip_list]
for i,val in enumerate(ip_list):
    master = val
    slaves = ip_list[:i] + ip_list[i+1:]
    slaves = '$'.join(slaves)
    os.system(f"docker run --net brokernet --ip {master} -d -e master {master} -e slave {slaves} --rm manager")