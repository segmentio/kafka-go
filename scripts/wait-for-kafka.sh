#/bin/bash

COUNTER=0; 
echo foo | nc localhost 9092
STATUS=$?
ATTEMPTS=60
until [ ${STATUS} -eq 0 ] || [ "$COUNTER" -ge "${ATTEMPTS}" ];
do
    let COUNTER=$COUNTER+1;
    sleep 1;
    echo "[$COUNTER] waiting for 9092 port to be open";
    echo foo | nc localhost 9092
    STATUS=$?
done

if [ "${COUNTER}" -gt "${ATTEMPTS}" ];
then
    echo "Kafka is not running, failing"
    exit 1
fi