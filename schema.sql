CREATE DATABASE  IF NOT EXISTS `trinistatsdb` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `trinistatsdb`;
-- MySQL dump 10.13  Distrib 5.7.29, for Linux (x86_64)
--
-- Host: 192.168.101.10    Database: trinistatsdb
-- ------------------------------------------------------
-- Server version	8.0.19

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `auth_group`
--

DROP TABLE IF EXISTS `auth_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(150) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_group_permissions`
--

DROP TABLE IF EXISTS `auth_group_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group_permissions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `group_id` int NOT NULL,
  `permission_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_group_permissions_group_id_permission_id_0cd325b0_uniq` (`group_id`,`permission_id`),
  KEY `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` (`permission_id`),
  CONSTRAINT `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
  CONSTRAINT `auth_group_permissions_group_id_b120cbf9_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_permission`
--

DROP TABLE IF EXISTS `auth_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_permission` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `content_type_id` int NOT NULL,
  `codename` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_permission_content_type_id_codename_01ab375a_uniq` (`content_type_id`,`codename`),
  CONSTRAINT `auth_permission_content_type_id_2f476e4b_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user` (
  `id` int NOT NULL AUTO_INCREMENT,
  `password` varchar(128) NOT NULL,
  `last_login` datetime(6) DEFAULT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(150) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(150) NOT NULL,
  `email` varchar(254) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_groups`
--

DROP TABLE IF EXISTS `auth_user_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_groups` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `group_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_groups_user_id_group_id_94350c0c_uniq` (`user_id`,`group_id`),
  KEY `auth_user_groups_group_id_97559544_fk_auth_group_id` (`group_id`),
  CONSTRAINT `auth_user_groups_group_id_97559544_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `auth_user_groups_user_id_6a12ed8b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_user_permissions`
--

DROP TABLE IF EXISTS `auth_user_user_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_user_permissions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `permission_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_user_permissions_user_id_permission_id_14a6b632_uniq` (`user_id`,`permission_id`),
  KEY `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` (`permission_id`),
  CONSTRAINT `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
  CONSTRAINT `auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `covid19cases`
--

DROP TABLE IF EXISTS `covid19cases`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `covid19cases` (
  `idcases` int NOT NULL AUTO_INCREMENT,
  `date` datetime NOT NULL,
  `numtested` int unsigned NOT NULL,
  `numpositive` int unsigned NOT NULL,
  `numdeaths` int unsigned NOT NULL,
  `numrecovered` int unsigned NOT NULL,
  PRIMARY KEY (`idcases`),
  UNIQUE KEY `idcases_UNIQUE` (`idcases`),
  UNIQUE KEY `date_UNIQUE` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `covid19dailydata`
--

DROP TABLE IF EXISTS `covid19dailydata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `covid19dailydata` (
  `idcovid19dailydata` int NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `dailytests` int NOT NULL DEFAULT '0',
  `dailypositive` int NOT NULL DEFAULT '0',
  `dailydeaths` int NOT NULL DEFAULT '0',
  `dailyrecovered` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`idcovid19dailydata`),
  UNIQUE KEY `date_UNIQUE` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=3755 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dailyequitysummary`
--

DROP TABLE IF EXISTS `dailyequitysummary`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dailyequitysummary` (
  `equitytradeid` int unsigned NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `equityid` smallint unsigned NOT NULL,
  `openprice` decimal(12,2) unsigned DEFAULT NULL,
  `high` decimal(12,2) unsigned DEFAULT NULL,
  `low` decimal(12,2) unsigned DEFAULT NULL,
  `osbid` decimal(12,2) unsigned DEFAULT NULL,
  `osbidvol` int unsigned DEFAULT NULL,
  `osoffer` decimal(12,2) unsigned DEFAULT NULL,
  `osoffervol` int unsigned DEFAULT NULL,
  `saleprice` decimal(12,2) unsigned DEFAULT NULL,
  `closeprice` decimal(12,2) unsigned DEFAULT NULL,
  `volumetraded` int unsigned DEFAULT NULL,
  `valuetraded` decimal(20,2) unsigned DEFAULT NULL,
  `changedollars` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`equitytradeid`),
  UNIQUE KEY `uniqueindex` (`date`,`equityid`),
  KEY `equityid_idx` (`equityid`),
  CONSTRAINT `fk_dailyequitysummary_equityid` FOREIGN KEY (`equityid`) REFERENCES `listedequities` (`equityid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=31321 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dividendyield`
--

DROP TABLE IF EXISTS `dividendyield`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dividendyield` (
  `dividendyieldid` int unsigned NOT NULL AUTO_INCREMENT,
  `yielddate` date NOT NULL,
  `yieldpercent` decimal(20,5) unsigned NOT NULL,
  `equityid` smallint unsigned NOT NULL,
  PRIMARY KEY (`dividendyieldid`),
  UNIQUE KEY `dividendyieldid_UNIQUE` (`yielddate`,`equityid`),
  KEY `fk_dividendyield_1_idx` (`equityid`),
  CONSTRAINT `fk_dividendyield_1` FOREIGN KEY (`equityid`) REFERENCES `listedequities` (`equityid`)
) ENGINE=InnoDB AUTO_INCREMENT=341 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_admin_log`
--

DROP TABLE IF EXISTS `django_admin_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_admin_log` (
  `id` int NOT NULL AUTO_INCREMENT,
  `action_time` datetime(6) NOT NULL,
  `object_id` longtext,
  `object_repr` varchar(200) NOT NULL,
  `action_flag` smallint unsigned NOT NULL,
  `change_message` longtext NOT NULL,
  `content_type_id` int DEFAULT NULL,
  `user_id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `django_admin_log_content_type_id_c4bce8eb_fk_django_co` (`content_type_id`),
  KEY `django_admin_log_user_id_c564eba6_fk_auth_user_id` (`user_id`),
  CONSTRAINT `django_admin_log_content_type_id_c4bce8eb_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`),
  CONSTRAINT `django_admin_log_user_id_c564eba6_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_content_type`
--

DROP TABLE IF EXISTS `django_content_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_content_type` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_label` varchar(100) NOT NULL,
  `model` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `django_content_type_app_label_model_76bd3d3b_uniq` (`app_label`,`model`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_migrations`
--

DROP TABLE IF EXISTS `django_migrations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_migrations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `applied` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_session`
--

DROP TABLE IF EXISTS `django_session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_session` (
  `session_key` varchar(40) NOT NULL,
  `session_data` longtext NOT NULL,
  `expire_date` datetime(6) NOT NULL,
  PRIMARY KEY (`session_key`),
  KEY `django_session_expire_date_a5c62663` (`expire_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `historicaldividendinfo`
--

DROP TABLE IF EXISTS `historicaldividendinfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `historicaldividendinfo` (
  `historicaldividendid` int unsigned NOT NULL AUTO_INCREMENT,
  `recorddate` date NOT NULL,
  `dividendamount` decimal(20,5) unsigned NOT NULL,
  `equityid` smallint unsigned NOT NULL,
  `currency` varchar(6) DEFAULT 'TTD',
  PRIMARY KEY (`historicaldividendid`),
  UNIQUE KEY `uniqueindex` (`recorddate`,`equityid`),
  KEY `equiyid_idx` (`equityid`),
  CONSTRAINT `equiyid` FOREIGN KEY (`equityid`) REFERENCES `listedequities` (`equityid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2610 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `historicalmarketsummary`
--

DROP TABLE IF EXISTS `historicalmarketsummary`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `historicalmarketsummary` (
  `summaryid` int unsigned NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `compositetotalsindexvalue` decimal(20,2) unsigned NOT NULL,
  `compositetotalsindexchange` decimal(10,2) NOT NULL,
  `compositetotalschange` decimal(7,2) NOT NULL,
  `compositetotalsvolumetraded` int unsigned NOT NULL,
  `compositetotalsvaluetraded` decimal(23,2) unsigned NOT NULL,
  `compositetotalsnumtrades` int unsigned NOT NULL,
  `alltnttotalsindexvalue` decimal(20,2) unsigned NOT NULL,
  `alltnttotalsindexchange` decimal(10,2) NOT NULL,
  `alltnttotalschange` decimal(7,2) NOT NULL,
  `alltnttotalsvolumetraded` int unsigned NOT NULL,
  `alltnttotalsvaluetraded` decimal(23,2) unsigned NOT NULL,
  `alltnttotalsnumtrades` int unsigned NOT NULL,
  `crosslistedtotalsindexvalue` decimal(20,2) unsigned NOT NULL,
  `crosslistedtotalsindexchange` decimal(10,2) NOT NULL,
  `crosslistedtotalschange` decimal(7,2) NOT NULL,
  `crosslistedtotalsvolumetraded` int unsigned NOT NULL,
  `crosslistedtotalsvaluetraded` decimal(23,2) unsigned NOT NULL,
  `crosslistedtotalsnumtrades` int unsigned NOT NULL,
  `smetotalsindexvalue` decimal(20,2) unsigned DEFAULT NULL,
  `smetotalsindexchange` decimal(10,2) DEFAULT NULL,
  `smetotalschange` decimal(7,2) DEFAULT NULL,
  `smetotalsvolumetraded` int unsigned DEFAULT NULL,
  `smetotalsvaluetraded` decimal(23,2) unsigned DEFAULT NULL,
  `smetotalsnumtrades` int unsigned DEFAULT NULL,
  `mutualfundstotalsvolumetraded` int unsigned DEFAULT NULL,
  `mutualfundstotalsvaluetraded` decimal(23,2) unsigned DEFAULT NULL,
  `mutualfundstotalsnumtrades` int unsigned DEFAULT NULL,
  `secondtiertotalsnumtrades` int unsigned DEFAULT NULL,
  PRIMARY KEY (`summaryid`),
  UNIQUE KEY `date_UNIQUE` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=3467 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `historicalstockinfo`
--

DROP TABLE IF EXISTS `historicalstockinfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `historicalstockinfo` (
  `historicalstockid` int unsigned NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `equityid` smallint unsigned NOT NULL,
  `closingquote` decimal(20,2) unsigned DEFAULT NULL,
  `changedollars` decimal(20,2) DEFAULT NULL,
  `volumetraded` int unsigned DEFAULT NULL,
  `currency` varchar(6) NOT NULL DEFAULT 'TTD',
  PRIMARY KEY (`historicalstockid`),
  UNIQUE KEY `uniqueindex` (`date`,`equityid`),
  KEY `equityid_idx` (`equityid`),
  CONSTRAINT `equityid` FOREIGN KEY (`equityid`) REFERENCES `listedequities` (`equityid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=379214 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `listedequities`
--

DROP TABLE IF EXISTS `listedequities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `listedequities` (
  `equityid` smallint unsigned NOT NULL AUTO_INCREMENT,
  `securityname` varchar(100) NOT NULL,
  `symbol` varchar(20) NOT NULL,
  `status` varchar(20) DEFAULT NULL,
  `sector` varchar(100) DEFAULT NULL,
  `issuedsharecapital` bigint unsigned DEFAULT NULL,
  `marketcapitalization` decimal(25,5) unsigned DEFAULT NULL,
  PRIMARY KEY (`equityid`),
  UNIQUE KEY `equityid_UNIQUE` (`equityid`),
  UNIQUE KEY `symbol_UNIQUE` (`symbol`)
) ENGINE=InnoDB AUTO_INCREMENT=176 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-04-08 15:54:40
