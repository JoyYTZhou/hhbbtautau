#! transfer all files
pwd
echo "List all files in $OUTPUTPATH"
ls -lR $OUTPUTPATH
echo "*******************************************"
OUTDIR=root://cmseos.fnal.gov//store/user/username/MyCondorOutputArea/
echo "xrdcp output for condor to "
echo $CONDORPATH


for FILE in
do
    echo "xrdcp -f ${FILE} ${OUTDIR}/${FILE}"
    echo "${FILE}"
    echo "${OUTDIR}"
    xrdcp -f ${FILE} ${OUTDIR}/${FILE} 2>&1
          XRDEXIT=$?
               if [[ $XRDEXIT -ne 0 ]]; then
                       rm *.root
                           echo "exit code $XRDEXIT, failure in xrdcp"
                               exit $XRDEXIT
                                 fi
                                   rm ${FILE}
                               done
