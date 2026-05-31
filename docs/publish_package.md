# Publish to maven repo
mvn --settings=settings.xml package deploy -Dgpg.keyname=AB61BB6174E37D31029A6EB6327FE43384A95185 -Dgpg.passphrase=