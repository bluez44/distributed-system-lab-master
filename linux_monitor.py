import subprocess
command = "ps aux | grep python"
result = subprocess.run(command, shell=True, capture_output=True, text=True)
print(result.stdout)