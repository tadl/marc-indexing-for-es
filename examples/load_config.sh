if [ -e config.sh ];
then
. config.sh
else
echo "missing config.sh -- try copying config-example.sh to config.sh and editing the contents"
exit
fi
