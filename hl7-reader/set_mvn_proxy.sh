# you must source this file

_https_proxy_parts="$(echo $https_proxy | tr ':/' '  ')"
_https_proxy_host="$(echo $_https_proxy_parts | awk '{print $2}')"
_https_proxy_port="$(echo $_https_proxy_parts | awk '{print $3}')"

export MAVEN_OPTS="-Dhttps.proxySet=true  -Dhttps.proxyHost=$_https_proxy_host -Dhttps.proxyPort=$_https_proxy_port"
