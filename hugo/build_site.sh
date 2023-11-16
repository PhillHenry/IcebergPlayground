rm -rf public/* && hugo -verbose && ls public/ && \
for FILE in $(find public/ -name \*.xml) ; do { rm $FILE ; } done && \
rm -rf ../docs && mv public ../docs  && \
for FILE in $(find ../docs/) ; do { echo Adding $FILE ; git add $FILE ; } done && git commit ../docs -m "rebuilt site" && \
echo Now run git push