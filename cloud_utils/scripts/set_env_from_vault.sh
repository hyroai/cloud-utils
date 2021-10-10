#!/bin/bash

function getToken(){
  local response=$(curl 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fmanagement.azure.com%2F' -H Metadata:true -s | jq -r .access_token)
  echo "$response"
}

function exitIfEmpty(){
  if [ -z "$1" ]
  then
    echo "$2" is empty. Aborting. "$1"
    exit 1
  fi
}

metadata=$(curl -H Metadata:true "http://169.254.169.254/metadata/instance?api-version=2019-08-15")
exitIfEmpty "$metadata" metadata

subscription_id=$(echo $metadata | jq -r .compute.subscriptionId)
vm_name=$(echo $metadata | jq -r .compute.name)
vmss_name=$(echo $metadata | jq -r .compute.vmScaleSetName)
resource_group_name=$(echo $metadata | jq -r .compute.resourceGroupName)

i="0"

while [ $i -lt 10 ]
do
  jwt="$(getToken)"
  if [ -z "$jwt" ]
  then
    sleep 60 # Pause before retry
    i=$((i+1))
  else
    i=10
  fi
done

cat <<EOF > auth_payload_complete.json
{
    "role": "${ROLE:-$VAULT_KEY-role}",
    "jwt": "$jwt",
    "subscription_id": "$subscription_id",
    "resource_group_name": "$resource_group_name",
    "vm_name": "$vm_name",
    "vmss_name": "$vmss_name"
}
EOF

export VAULT_SKIP_VERIFY=true
valut_login=$(curl --request POST --data @auth_payload_complete.json "$VAULT_HOST/v1/auth/azure/login")
exitIfEmpty "$valut_login" valut_login
token=$(echo "$valut_login" | jq -r '.auth.client_token')

valut_keys=$(curl -H "X-Vault-Token: $token" -X GET "$VAULT_HOST/v1/secret/data/$VAULT_KEY")
exitIfEmpty "$valut_keys" valut_keys
mkfifo injectenv
echo "$valut_keys" |
jq -r '.data.data' |
jq -r 'to_entries|map("\(.key)=\(.value|tostring)")|.[]' > injectenv &

while read -r i; do
  export "${i?}"
done < injectenv
printenv