# Clone the different policies (from different branches) for OpenDC locally to start building them
# https://stackoverflow.com/questions/17383217/git-clone-from-a-branch-other-than-master
# Store the root folder inside which all the metric outputs would be stored
begin=$(date +"%s")
echo "----------------------Start of the script-----------------------"
echo "Setting up the base directory for further operation"
HOME_DIR=`eval echo ~$USER`
OPENDC_ENV="$HOME_DIR/OpenDC Test Automation"
basepath=$OPENDC_ENV
echo "Trying to create $basepath directory, if it does not exist..."
if [ -d "$OPENDC_ENV" ];
then
    echo "The directory $OPENDC_ENV is already there. Do you want to go ahead and delete? Press ENTER -> Yes, Ctrl+C -> Exit!"
    read
    rm -rf "$OPENDC_ENV";
    echo "Deleted the directory $basepath, to start afresh!"
fi
mkdir -p "$OPENDC_ENV"
cd "$OPENDC_ENV"
echo "Created $OPENDC_ENV directory, that will be the basepath of all policies of OpenDC."
echo "-------------------MAX-MIN--------------------"
# Taking Maxi's policy (Max-Min) first, and trying to generate the build using Gradle
mkdir "Max-Min"
echo "Generated a separate directory Max-Min -> Max-Min policy"
cd "Max-Min" # cd "$basepath" to go to the starting location
echo "Cloning Max-Min policy from the respective branch of OpenDC repository into this new directory"
git clone -b MaxMin_v2 --single-branch https://github.com/CloudScheduling/opendc.git
# Navigating to the path of WorkflowServiceTest.kt class for building and executing the tests
# cd "opendc/opendc-workflow/opendc-workflow-service"
# To query tests in the project - gradle :opendc-workflow:opendc-workflow-service:tasks
# cd to the OpenDC project for each branch before running the tests - /home/amihelpful/OpenDC Test Automation/Max-Min/opendc
cd opendc
echo "Performing gradle build to execute the tests - Max-Min policy"
gradle -q :opendc-workflow:opendc-workflow-service:test
echo "The respective CSVs have been generated and placed in the Max-Min directory!"
cd ../..
echo "Returned to the base-path"
echo "----------------------------------------------"
echo "-------------------MIN-MIN--------------------"
# Taking Florian's policy (Min-Min) next, and trying to generate the build using Gradle
mkdir "Min-Min"
echo "Generated a separate directory Min-Min -> Min-Min policy"
cd "Min-Min" # cd "$basepath" to go to the starting location
echo "Cloning Min-Min policy from the respective branch of OpenDC repository into this new directory"
git clone -b MinMinFlorian --single-branch https://github.com/CloudScheduling/opendc.git
cd opendc
echo "Performing gradle build to execute the tests - Min-Min policy"
gradle -q :opendc-workflow:opendc-workflow-service:test
echo "The respective CSVs have been generated and placed in the Min-Min directory!"
cd ../..
echo "Returned to the base-path"
echo "----------------------------------------------"
echo "--------------------ELoP----------------------"
# Taking Linus' policy (ELOP) next, and trying to generate the build using Gradle
mkdir "ELoP"
echo "Generated a separate directory ELoP -> ELoP policy"
cd "ELoP" # cd "$basepath" to go to the starting location
echo "Cloning ELoP policy from the respective branch of OpenDC repository into this new directory"
git clone -b Policy_ELoP --single-branch https://github.com/CloudScheduling/opendc.git
cd opendc
echo "Performing gradle build to execute the tests - ELoP policy"
gradle -q :opendc-workflow:opendc-workflow-service:test
echo "The respective CSVs have been generated and placed in the ELoP directory!"
cd ../..
echo "Returned to the base-path"
echo "----------------------------------------------"
echo "-------------------HEFT--------------------"
# Taking Shekhar's policy (HEFT) next, and trying to generate the build using Gradle
mkdir "HEFT"
echo "Generated a separate directory HEFT -> HEFT policy"
cd "HEFT" # cd "$basepath" to go to the starting location
echo "Cloning HEFT policy from the respective branch of OpenDC repository into this new directory"
git clone -b HEFT_Policy --single-branch https://github.com/CloudScheduling/opendc.git
cd opendc
echo "Performing gradle build to execute the tests - HEFT policy"
gradle -q :opendc-workflow:opendc-workflow-service:test
echo "The respective CSVs have been generated and placed in the HEFT directory!"
cd ../..
echo "Returned to the base-path"
echo "----------------------------------------------"
echo "-------------------Random--------------------"
# Taking Random policy next, and trying to generate the build using Gradle
mkdir "Random"
echo "Generated a separate directory Random -> Random policy"
cd "Random" # cd "$basepath" to go to the starting location
echo "Cloning Random policy from the respective branch of OpenDC repository into this new directory"
git clone -b Random_Policy --single-branch https://github.com/CloudScheduling/opendc.git
cd opendc
echo "Performing gradle build to execute the tests - Random policy"
gradle -q :opendc-workflow:opendc-workflow-service:test
echo "The respective CSVs have been generated and placed in the Random directory!"
cd ../..
echo "Returned to the base-path"
echo "----------------------End of the script-----------------------"
end=$(date +"%s")
runtime=$(($end-$begin))
#echo "Total seconds taken by the script - $runtime"
echo "$(($runtime/60)) minutes and $(($runtime % 60)) seconds elapsed during script execution..."
