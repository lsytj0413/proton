#!/bin/bash -x

usage() {
    echo "$PROGNAME: usage: $PROGNAME new_project_name"
    return
}

PROGNAME=$(basename $0)
BASEPATH=$(cd `dirname $0` && pwd)
dir=$(dirname ${BASEPATH})
nname=$1

if [[ -z $nname ]]; then
    usage >&2
    echo "MUST specify new project name"
    exit 1
fi

shift
while [[ -n $1 ]]; do
    usage >&2
    exit 1
done

echo "Rename the project to $nname, under directory: $dir"

find $dir -type f -not -path '*/\.git/*' -not -path '*rename\.sh*' -exec sed -i '' "s/golang-project-template/${nname}/g" {} \;