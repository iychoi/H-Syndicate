H-Syndicate
===========

H-Syndicate allows accesses to Syndicate Volumes via filesystem-like interfaces. There are two filesystem interfaces provided:
- hsyndicate.fs.SyndicateFileSystem: General filesystem interface in Java
- hsyndicate.hadoop.dfs.HSyndicateDFS: Implementation of Hadoop filesystem interface.

The H-Syndicate requires [Syndicate-UG HTTP/REST services](https://github.com/syndicate-storage/syndicate-ug-http) running. 

Building
--------

Building from the source code is very simple. Source code is written in Java and provides "NetBeans" project file and "Ant" scripts for building. If you are using NetBeans IDE, load the project and build through the IDE. Or, simple type "ant".

```
$ ant
```

All dependencies for this project are already in /libs/ directory.

Ansible
-------

```
ansible-playbook -i hosts.yml site.yml
```
