rm -rf public/* && hugo -verbose && ls public/ && \
for FILE in $(find public/ -name \*.xml) ; do { echo Deleting $FILE ; rm $FILE ; } done && \
for FILE in $(find ../docs/ | grep -v CNAME) ; do { echo Deleting $FILE ; rm $FILE ; } done && cp -r public/* ../docs/ && \
for FILE in $(find ../docs/) ; do { echo Adding $FILE ; git add $FILE ; } done && git commit ../docs -m "rebuilt site" && \
echo Now run git push
