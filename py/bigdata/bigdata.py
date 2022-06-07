from fabric import Connection

c = Connection("storage-0.storage")
result = c.run("uname -s")
print(result.stdout.strip())

# 1. stop service
# 2. mount remote
# 3. create hard link
# 4. start service
