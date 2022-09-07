# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM openjdk:11
COPY dspdm-services/dspdm.msp.mainservice/target/docker/javarun/ /javarun
COPY dspdm-services/src/main/resources/tiger4/localhost/ /javarun/config
RUN cat /javarun/config/*
WORKDIR /javarun
CMD ["java", "-DpathToConfigRootDir=/javarun","-agentlib:jdwp=transport=dt_socket,address=127.0.0.1:8888,server=y,suspend=n","-Xms2048m","-Xmx2048m","-cp","/javarun/*","com.lgc.dspdm.msp.mainservice.GrizzlyServer","-p","8080"]
