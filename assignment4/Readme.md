This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit.
Things to do after checking out and opening in intellij:
    Add scala SDK 2.11.11 (tested with this version, it has to be 2.11.x since this is the spark version)
    Add jdk 8
    add Framework support of Maven
To build the project:
    run mvn package