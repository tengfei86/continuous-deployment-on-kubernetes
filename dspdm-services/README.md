# dspdm-service
# Instructions to run unit tests


# 1. Install java open jdk 11 from https://jdk.java.net/java-se-ri/11
# 2. Install maven latest version from http://maven.apache.org/download.cgi

# How to Build the DSPDM main services
# Step 1:
    Copy the following files from directory /src/main/resources/noauth to the path C:/DSPDM/config.
     1. config.properties
     2. connection.properties
     3. log4j2.xml
     4. security-server.properties
     5. wafbypassrules.properties

    Alternatively contents of two files connection.properties and config.properties can be exported as environment variables.
    If you want to serve these config files from a different directory then 
# Step 2:
      Open config.properties file from the external directory and edit it and set is_ecurity_enabled=false if not already set to false
# Step 3:
      You must already be fixed with all the database connection settings inside the connection.properties file.
# Step 4:
      msp_environment directory must exist inside the C:/DSPDM if you want to use the mvn_build_localhost.bat script.
# Step 5:
      Build the application with unit tests enabled. Set the ConfigRootDirPath in params like this way
      -DConfigRootDirPath= C:/DSPDM
      Alternatively you can also use the batch script provided mvn_build_localhost.bat
# Step 6:
      After successful build you application will be auto started or you can also start it manually by using the following command
      start /D "C:/DSPDM/msp_environment" java -DSERVICE_ROOT_EXT=msp -DpathToConfigRootDir="C:/DSPDM" -agentlib:jdwp=transport=dt_socket,address=127.0.0.1:8888,server=y,suspend=n -Xms2048m -Xmx2048m -cp "C:/DSPDM/msp_environment/*" com.lgc.dist.core.msp.grizzly.GrizzlyServer -p 8086



