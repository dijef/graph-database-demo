/**
 * Copyright (c) 2019 Pawel Maslej
 * File created: 4 Jul 2019 by Pawel
 * Creator : Pawel Maslej
 * Version : $Id$
 */
package org.lhasalimited.pawel.graphdatabasedemo;

import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Pawel Maslej
 * @since 4 Jul 2019
 */
public class OrientFunctions
{
	/**
	 * TinkerPop API access
	 */
	private Graph graph;

	/**
	 * Full database access
	 */
	private OrientGraph orientGraph;

	public OrientFunctions(OrientGraph orientGraph) {
		this.orientGraph = orientGraph;
		this.graph = orientGraph;
	}

	public void demoOrientSpecificFeatures() {
		/*
		 * Specify data for field 'name' to Double while already String data is present
		 */
		var schema = orientGraph.database().getMetadata().getSchema();
		OClass alertClass = schema.getClass("alert"); // class must exists already or be created
		printCantChangeAlertField(alertClass);

		/*
		 * Constrain data for field 'name' to String only.
		 */
		alertClass.createProperty("name", OType.STRING);
		graph.tx().commit();
		printStringRestrictionIsNotQuiteWorking(alertClass);

		/*
		 * Create index to speed up searching by name (tested working on other project)
		 */
		alertClass.createIndex("index_name", INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name");
		graph.tx().commit();

		/*
		 * Define inheritance. This covers database meta and not TinkerPop itself
		 */
		orientGraph.createClass("alertChild", "alert");
		graph.tx().commit();
		Vertex vAlertChild = graph.addVertex(T.label, "alertChild", "name", "Child alert", "child", "yes");
		graph.tx().commit();
		System.out.println("Created alertchild: " + vAlertChild);
		printAlertChildByAlertLabel();
	}

	private void printCantChangeAlertField(OClass alertClass) {
		try
		{
			alertClass.createProperty("name", OType.DOUBLE);
		}
		catch (Exception e)
		{
			System.out.println("Failed to changed data type of property name: " + e.getMessage());
		}
		graph.tx().commit();
	}

	private void printStringRestrictionIsNotQuiteWorking(OClass alertClass) {
		Vertex v = graph.traversal()
				.V()
					.hasLabel("alert")
				.next();
		v.property("name", 150D);
		graph.tx().commit();

		System.out.println("Constraining 'alert' to String data only did not quite work as Double was auto converted to : " + graph.vertices(v.id()).next().property("name").value().getClass().getSimpleName());
		graph.tx().commit();
	}

	public void printAlertChildByAlertLabel() {
		List<Vertex> vList = graph.traversal()
			.V()
				.hasLabel("alert", getOrientSubClasses("alert")) // all labels must be listed
			.toList();
		System.out.println("Searched for alerts and included child alerts via inheritance: ");
		vList.forEach(System.out::println);
		System.out.println();
		graph.tx().commit();
	}

	public void printDemoSchema() {
		System.out.println("\nPrinting all schema:");
		var schema = orientGraph.database().getMetadata().getSchema();

		System.out.print("All classes: ");
		schema.getClasses().stream().map(OClass::getName).forEach(name -> System.out.print(name + ' '));
		System.out.println();

		System.out.print("Defined properties and indexes for alert: ");
		OClass alertClass = schema.getClass("alert");
		alertClass.propertiesMap().keySet().forEach(name -> System.out.print(name + ' '));
		alertClass.getIndexedProperties().stream().map(OProperty::getName).forEach(name -> System.out.print(name + ' '));
	}

	public String[] getOrientSubClasses(String className) {
		OClass oClass = orientGraph.database().getMetadata().getSchema().getClass(className);
		return oClass.getSubclasses().stream().map(OClass::getName).toArray(String[]::new);
	}
}