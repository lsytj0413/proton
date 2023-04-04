SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Proto Directory: " $SRC_DIR

DST_DIR=$( dirname $SRC_DIR )
DST_DIR=${DST_DIR}/pb
echo "Dest Directory: " $DST_DIR

rm -rvf ${DST_DIR}
mkdir -p ${DST_DIR}

protoc \
  -I=$SRC_DIR \
  --go_out=${DST_DIR} \
  --go_opt=paths=source_relative \
  --go-grpc_out=${DST_DIR} \
  --go-grpc_opt=paths=source_relative \
  --grpc-gateway_out ${DST_DIR} \
  --grpc-gateway_opt logtostderr=true \
  --grpc-gateway_opt paths=source_relative \
  --grpc-gateway_opt generate_unbound_methods=true \
  ${SRC_DIR}/api.proto
