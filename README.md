# webHDFSClientBundle

## Description

webHDFSClientBundle provides a Symfony2 wrapper to the SimpleEnergy/php-WebHDFS code for interacting with HDFS from PHP.
php-WebHDFS is a PHP client for [WebHDFS](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).


## Dependencies
* [PHP](http://php.net/)
* [cURL](http://curl.haxx.se/)
* [Hadoop 2.x] (http://hadoop.apache.org/)
* [HDFS with Web API enabled] (http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
* [Symfony2](http://symfony.com/)


## Installation

Using composer
--------------

To install webHDFSClientBundle with Composer add the following to your `composer.json` file:

```js
// composer.json
{
    // ...
    require: {
        // ...
        "jimbglenn/webHDFSClientBundle": "dev-master"
    }
}
```

Then, you can install with Composer's ``update`` command from your project root:
```bash
composer update jimbglenn/webHDFSClientBundle
```

Composer will download all the required files, and install them.  Now you update ``AppKernel.php`` file and register the
new bundle:

```php

<?php
// in AppKernel::registerBundles()
    $bundles = array(
        // ...
        new jimbglenn\webHDFSClientBundle\webHDFSClientBundle(),
        // ...
        );
```

Setup config for your instance of Hadoop's WebHDFS api. In app/config/config.yml add the following section and
fill in with your values:
```
# web hdfs client configuration for hadoop:
web_hdfs_client:
    webHDFS:
        serverName: devhadoop.localdomain
        port: 50070
        user: hadoop
```

## Example:

From within a controller, you should be able to do get access to the web_hdfs_client:
```php
$hdfs = $this->get('web_hdfs_cilent');
```

## Usage

- :doc:`Read the documentation`

### File and Directory Operations

#### Create and Write to a File
```php
$hdfs = $this->get('web_hdfs_client');
$hdfs->create('user/hadoop-username/new-file.txt', 'local-file.txt');
```


