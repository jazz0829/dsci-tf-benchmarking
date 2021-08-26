#!/bin/sh
mkdir -p ~/.aws
#echo -e "[ci-global]" > ~/.aws/credentials
echo -e "[ci-glbl-auto]" > ~/.aws/credentials
echo -e "aws_access_key_id = $access_key_ci_glbl_auto" >> ~/.aws/credentials
echo -e "aws_secret_access_key = $secret_key_ci_glbl_auto" >> ~/.aws/credentials

eval wkspc_access_key='$'access_key_ci_"$WKSPC"
eval wkspc_secret_key='$'secret_key_ci_"$WKSPC"

echo -e "[default]" >> ~/.aws/credentials
echo -e "aws_access_key_id = $wkspc_access_key" >> ~/.aws/credentials
echo -e "aws_secret_access_key = $wkspc_secret_key" >> ~/.aws/credentials

if [[ "$WKSPC" = "dd" ]]; then 
    echo -e "[mgmt]" >> ~/.aws/credentials
    echo -e "aws_access_key_id = $access_key_ci_dd" >> ~/.aws/credentials
    echo -e "aws_secret_access_key = $secret_key_ci_dd" >> ~/.aws/credentials

elif
    [[ "$WKSPC" = "dt" ]]; then 
    echo -e "[mgmt]" >> ~/.aws/credentials
    echo -e "aws_access_key_id = $access_key_ci_dt" >> ~/.aws/credentials
    echo -e "aws_secret_access_key = $secret_key_ci_dt" >> ~/.aws/credentials

elif [[ "$WKSPC" = "dp" ]]; then 
    echo -e "[mgmt]" >> ~/.aws/credentials
    echo -e "aws_access_key_id = $access_key_ci_dp" >> ~/.aws/credentials
    echo -e "aws_secret_access_key = $secret_key_ci_dp" >> ~/.aws/credentials


cat ~/.aws/credentials;
fi