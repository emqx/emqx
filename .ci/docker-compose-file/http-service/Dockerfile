FROM tomcat:10.0.5

RUN wget https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip \
	&& unzip -q apache-maven-3.6.3-bin.zip \
	&& mv apache-maven-3.6.3 /opt/apache-maven-3.6.3/ \
	&& ln -s /opt/apache-maven-3.6.3/ /opt/maven
ENV M2_HOME=/opt/maven
ENV M2=$M2_HOME/bin
ENV PATH=$M2:$PATH
COPY ./web-server /code
WORKDIR /code
RUN mvn package -Dmaven.skip.test=true
RUN mv ./target/emqx-web-0.0.1.war /usr/local/tomcat/webapps/emqx-web.war
EXPOSE 8080
CMD ["/usr/local/tomcat/bin/catalina.sh","run"]
