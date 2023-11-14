#! transfer all files
pwd
echo "List all files in $OUTPUTPATH"
ls -lR $OUTPUTPATH
echo "*******************************************"
OUTDIR=root://cmseos.fnal.gov//store/user/username/MyCondorOutputArea/
echo "xrdcp output for condor to "
echo $CONDORPATH

src=$OUTPUTPATH
dest=$CONDORPATH

find "$src" -type f | while read -r file; do
    relpath="${file#$src}"
    xrdcp "$file" "$dest$relpath"
done
