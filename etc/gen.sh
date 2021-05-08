#!/bin/sh

for file in docs/*.conf;
do
    basef=$(basename $file)
    sed 's/^[[:blank:]]*##==/==/;/^[[:blank:]]*##/d;s/#[^@].*//;/^[[:blank:]]*$/d;s/#@/#/;s/^==*=$//;' $file | sed '/^$/N;/\n$/D;' > ./$basef
    if [ -z "$(head -1 ${basef})" ]
    then
        sed '1d' ./$basef > ./$basef.0
        mv ./$basef.0 ./$basef
    fi
done
