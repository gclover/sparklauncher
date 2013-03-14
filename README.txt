

Spark run file should edit:


exec  "$RUNNER" "-Xbootclasspath/p:$FWDIR/jsr166.jar" -cp "$CLASSPATH" $EXTRA_ARGS "$@"

Spark local file should edit:

exec  "$RUNNER" -cp "$CLASSPATH" org.apache.hadoop.util.RunJar "$@"