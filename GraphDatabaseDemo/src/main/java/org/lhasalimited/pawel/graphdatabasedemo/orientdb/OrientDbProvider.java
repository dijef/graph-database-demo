/**
 * Copyright (c) 2019 Pawel Maslej
 * File created: 1 Jul 2019 by Pawel
 * Creator : Pawel Maslej
 * Version : $Id$
 */
package org.lhasalimited.pawel.graphdatabasedemo.orientdb;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

/**
 * @author Pawel Maslej
 * @since 1 Jul 2019
 */
public class OrientDbProvider
{
	public static OrientGraph getEmbeddedOrientDb(String dbPath, String dbUser, String dbPassword) {
		OrientDbConfig config = new OrientDbConfig(dbPath, dbUser, dbPassword);
		OrientGraphFactory factory = newEmbeddedOrientFactory(config);
		OrientGraph graph = newOrientGraph(factory);
		return graph;
	}

	public static OrientGraph getServerOrientDb(String host, String dbName, String dbUser, String dbPassword) {
		String orientUrl = "remote:" + host;
		OrientDB orientDB = new OrientDB(orientUrl, dbUser, dbPassword, OrientDBConfig.defaultConfig());
		orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL); // configure database for first time

		OrientGraphFactory factory = newServerOrientFactory(orientDB, dbName, dbUser, dbPassword);
		OrientGraph graph = newOrientGraph(factory);
		return graph;
	}

	private static OrientGraphFactory newEmbeddedOrientFactory(OrientDbConfig config) {
		String orientUrl = "plocal:/tmp/" + config.getDbPath();
		OrientGraphFactory factory = new OrientGraphFactory(orientUrl, config.getDbUser(), config.getDbPassword());
		return factory;
	}

	private static OrientGraphFactory newServerOrientFactory(OrientDB orientDB, String dbName, String dbUser, String dbPassword) {
		return new OrientGraphFactory(orientDB, dbName, ODatabaseType.PLOCAL, dbUser, dbPassword);
	}

	private static OrientGraph newOrientGraph(OrientGraphFactory factory) {
		BaseConfiguration config = new BaseConfiguration();
		config.setProperty("orient-transactional", true); // enables transactions support
		//TODO: This seems not working
		config.setProperty("blueprints.orientdb.autoStartTx", false); // disables auto-start of transaction when add/remove
		OrientGraph	graph = new OrientGraph(factory, config, true);
		return graph;
	}

	static class OrientDbConfig
	{
		private String dbPath;
		private String dbUser;
		private String dbPassword;

		public OrientDbConfig(String dbPath, String dbUser, String dbPassword) {
			this.dbPath = dbPath;
			this.dbUser = dbUser;
			this.dbPassword = dbPassword;
		}

		public String getDbPath() {
			return dbPath;
		}

		public String getDbUser() {
			return dbUser;
		}

		public String getDbPassword() {
			return dbPassword;
		}
	}
}