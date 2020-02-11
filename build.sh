#/bin/sh
ORIG=$(pwd)
SRC=${ORIG}/src
DEST=${ORIG}/dest

mkdir ${DEST}
rm ${DEST}/function.zip

cd ${SRC}
chmod -R 755
zip -r9 ${DEST}/function.zip .
cd ${ORIG}

aws --region=$1 --profile $2 \
    lambda update-function-code \
    --function-name $3 \
    --zip-file fileb://${DEST}/function.zip

