#example usage
java -jar build\dist\labkeyMultiSiteQuery.jar -h

java -jar build\dist\labkeyMultiSiteQuery.jar --xml example\Example.xml -o output.txt -u username -p password

#debugging
java -Xdebug -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=4040 -jar build\dist\labkeyMultiSiteQuery.jar --xml example\Example.xml -o output.txt -u username -p password