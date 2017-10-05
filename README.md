H-Syndicate
===========

H-Syndicate allows accesses to Syndicate Volumes via filesystem-like interfaces. There are two filesystem interfaces provided:
- hsyndicate.fs.SyndicateFileSystem: General filesystem interface in Java
- hsyndicate.hadoop.dfs.HSyndicateDFS: Implementation of Hadoop filesystem interface.

The H-Syndicate requires [Syndicate-UG HTTP/REST services](https://github.com/syndicate-storage/syndicate-ug-http) running.

Building
--------

If you are using `NetBeans IDE`, load the project and build through the IDE. For most users, use `maven` to build the package from source.

```
$ mvn install
```

All dependencies for this project are already in /libs/ directory.

Configuration
-------------

First, set `classpath` of Java/Hadoop to make Hadoop be able to find the H-Syndicate package. The easiest way of doing this is copying the H-Syndicate jar package (including its dependencies) into Hadoop's library path (under `$HADOOP_HOME/lib/`).

Next, configure `core-site.xml` file to plug the H-Syndicate module into the Hadoop. Below is an example of `core-site.xml`.
```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
	<property>
		<name>fs.default.name</name>
		<value>hdfs://MASTER:8020</value>
	</property>
  <!-- H-Syndicate -->
	<property>
	  <!-- Plug-in the H-Syndicate module and bind it to hsyn:// scheme in file path -->
		<name>fs.hsyn.impl</name>
		<value>hsyndicate.hadoop.dfs.HSyndicateDFS</value>
		<description>hsyn protocol mapping for H-Syndicate.</description>
	</property>
	<property>
	  <!-- Set addresses of hosts where Syndicate-UG-HTTP services are running-->
		<name>fs.hsyndicate.hosts</name>
		<value>HOST_NAME_1,HOST_NAME_2,HOST_NAME_3</value>
	</property>
	<property>
	  <!-- Set default port where Syndicate-UG-HTTP services are running -->
		<name>fs.hsyndicate.port</name>
		<value>8888</value>
	</property>
</configuration>
```

Configure a session key for Syndicate.
```
hadoop credential create fs.hsyndicate.session.key -value <SESSION_KEY> -provider jceks://hdfs/user/<username>/hsyndicate.jceks

```

Ansible
-------

```
ansible-playbook -i hosts.yml site.yml
```


Usage
-----

```
hadoop dfs -D hadoop.security.credential.provider.path=jceks://hdfs/user/<username>/hsyndicate.jceks -ls hsyn:///
```
