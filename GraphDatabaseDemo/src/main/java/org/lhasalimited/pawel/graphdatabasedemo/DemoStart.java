/**
 * Copyright (c) 2019 Pawel Maslej
 * File created: 1 Jul 2019 by Pawel
 * Creator : Pawel Maslej
 * Version : $Id$
 */
package org.lhasalimited.pawel.graphdatabasedemo;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.lhasalimited.pawel.graphdatabasedemo.orientdb.OrientDbProvider;

/**
 * Shows some basics of using TinkerPop.
 *
 * @author Pawel Maslej
 * @since 1 Jul 2019
 */
public class DemoStart
{
	/**
	 * TinkerPop API access
	 */
	private Graph graph;

	/**
	 * Orient database access
	 */
	private OrientFunctions orientFunctions;

	private DemoStart(OrientGraph orientGraph) {
		this.graph = orientGraph;
		this.orientFunctions = new OrientFunctions(orientGraph);
	}

	public static void main(String[] args) throws Exception {
		/*
		 * Remove database folder before start!.
		 *
		 * Configures database and provides OrientGraph and Graph.
		 * This can be done using embedded or server mode.
		 */
		DemoStart start = configureEmbeddedDB();
//		DemoStart start = configureServerDB();

		/**
		 * Some TinkerPop features
		 */
		start.demo();

		/*
		 * Some OrientDB features
		 */
		start.demoOrientDB();
	}

	/** Uses drive from project location. e.g. D:/tmp/ path is visible in logs */
	private static DemoStart configureEmbeddedDB() {
		OrientGraph orientGraph = OrientDbProvider.getEmbeddedOrientDb("OrientDatabaseDemo", "admin", "admin");
		return new DemoStart(orientGraph);
	}

	private static DemoStart configureServerDB() {
		OrientGraph orientGraph = OrientDbProvider.getServerOrientDb("localhost", "OrientDatabaseDemo", "root", "admin");
		return new DemoStart(orientGraph);
	}

	private void demo()	{
		/*
		 * Import data into database and show TinkerPop in action.
		 */
		demoImport();

		/*
		 * Retrieve data from database
		 */
		demoSimpleRetrieve();

		/*
		 * Adding edges
		 */
		demoAddEdge();

		/*
		 * Search by
		 */
		demoSearch();
	}

	private void demoImport() {
		/*
		 * Note: OrientDB opens transaction automatically by default almost on any iteraction.
		 * Note: Adding vertex of new label type, usually takes a bit longer as it creates schema behind.
		 */

		/*
		 * Example 1
		 * Add vertex and properties at once
		 */
		graph.addVertex(T.label, "alert", "identifier", "001", "name", "alpha-Halo ketone");
		graph.tx().commit();

		/*
		 * Example 2
		 * Add vertex and then properties (same for adding properties at any later stage)
		 */
		Vertex v = graph.addVertex("alert");
		v.property("identifier", "002");
		v.property("name", "Nitro group on aliphatic ring");
		graph.tx().commit();

		/*
		 * Example 3
		 * Add vertex and properties using Traversal API. As traversal is not remote, commit is still required.
		 */
		graph.traversal()
			.addV("alert")
				.property("identifier", "003")
				.property("name", "Organophosphorus ester")
			.iterate(); // end call to execute
		graph.tx().commit();

		/*
		 * Add other vertices
		 */
		graph.traversal()
			.addV("endpoint")
				.property("name", "Cholinesterase inhibition")
			.addV("endpoint")
				.property("name", "Irritation (of the eye)")
			.addV("endpoint")
				.property("name", "Mutagenicity")
			.iterate();
		graph.tx().commit();
	}

	private void demoSimpleRetrieve() {
		/*
		 * Traversal behaviour is not consistent always, code below may not retrieve full vertex, but its ID field only. This is database / remote mode specific.
		 */
		List<Vertex> list = graph.traversal()
			.V() // selects all vertices
				.hasLabel("endpoint")
				.toList();
		graph.tx().commit();

		/*
		 * Full data retrieve can be specified explicitly. Use of valueMap returns map with properties and list (that's why printout includes []).
		 */
		List<Map<String, Object>> list2 = graph.traversal()
			.V() // selects all vertices
				.hasLabel("endpoint")
				.valueMap() // explicit ask without IDs
				.toList();
			graph.tx().commit();

		/*
		 * Full data retrieve with IDs included.
		 */
		List<Map<Object, Object>> list3 = graph.traversal()
			.V() // selects all vertices
				.hasLabel("alert")
				.valueMap(true) // explicit, includes IDs
				.toList();
			graph.tx().commit();

		printDemoRetrieve(list2, list3);
	}

	private void demoAddEdge() {
		/*
		 * For demo purposes I didn't created edges immediately, so vertices IDs must be learnt first.
		 */
		List<Object> alertIds = graph.traversal().V().hasLabel("alert").id().toList();
		List<Object> endpointIds = graph.traversal().V().hasLabel("endpoint").id().toList();

		Vertex alert1 = graph.vertices(alertIds.get(0)).next();
		Vertex alert2 = graph.vertices(alertIds.get(1)).next();
		Vertex alert3 = graph.vertices(alertIds.get(2)).next();
		Vertex endpoint1 = graph.vertices(endpointIds.get(0)).next();
		Vertex endpoint2 = graph.vertices(endpointIds.get(1)).next();
		Vertex endpoint3 = graph.vertices(endpointIds.get(2)).next();

		/*
		 * Example 1
		 * This can be used if vertices are already obtained. This creates edge with direction OUT from alert1 to endpoint1.
		 * Knowledge of direction is supper important when dealing with edges.
		 * Edges can have properties.
		 */
		alert1.addEdge("alertEndpoint", endpoint1);
		graph.tx().commit();

		/*
		 * Example 2
		 * Using traversal API to add an edge. Note that it requires proving another traversal, which is embedded by using class __.
		 */
		graph.traversal()
			.V(alert2.id())
				.addE("alertEndpoint").to(__.V(endpoint2.id()))
			.V(alert2.id())
				.addE("alertEndpoint").to(__.V(endpoint3.id()))
			.V(alert3.id())
				.addE("alertEndpoint").to(__.V(endpoint3.id()))
			.iterate();
		graph.tx().commit();

		printDemoOutEdges(Stream.of(alert1, alert2, alert3).map(Vertex::id).toArray());
	}

	private void demoSearch() {
		/*
		 * Note: I'm returning directly Vertex for simplicity
		 */

		/*
		 * Example 1
		 * Simple search by parameter name and value on all vertices. We can filter by label to.
		 */
		Vertex v1 = graph.traversal()
			.V()
				.hasLabel("alert")
				.has("identifier", "002")
			.toList().get(0);
		graph.tx().commit();

		/*
		 * Example 2
		 * Search on linked endpoint name. Direction is really important. As it is outgoing of vertex A, outV points to A, while inV will be vertex B.
		 */
		List<Vertex> list2 = graph.traversal()
			.V()
				.hasLabel("alert")
				.where(__
						.outE("alertEndpoint")
						.inV()
							.has("name", "Mutagenicity"))
			.toList();
		graph.tx().commit();

		printDemoSearchResults(v1, list2);
	}

	private void demoOrientDB() {
		/**
		 * Shows how to constrain data, add index to speed up searching, define inheritance.
		 * All changes applied to schema need to be done once only, also they require classes to be created first (this can be done manually)
		 */
		orientFunctions.demoOrientSpecificFeatures();

		/*
		 * Orient supports schema for all data added, so it can be browsed
		 */
		orientFunctions.printDemoSchema();
	}

	private void printDemoRetrieve(List<Map<String, Object>> list2, List<Map<Object, Object>> list3) {
		System.out.println("Printing out retrieves");

		System.out.print("All endpoints: ");
		list2.forEach(map -> System.out.print(map.get("name") + " ,"));
		System.out.println();

		System.out.print("All alerts: ");
		list3.forEach(map -> System.out.print(String.format("%s:%s ,", map.get("name"), map.get(T.id))));
		System.out.println();
	}

	private void printDemoOutEdges(Object ... ids) {
		System.out.println("Printing out edges");
		/*
		 * This shows outgoing edges. Edge does not exist without both vertices present.
		 */
		for (var id : ids)
		{
			Vertex v = graph.vertices(id).next();
			System.out.println(v.edges(Direction.OUT, "alertEndpoint").next());
		}
	}

	private void printDemoSearchResults(Object o1, List<?> l2) {
		System.out.println("\nPrinting search results 1:");
		System.out.println(o1);

		System.out.println("\nPrinting search results 2:");
		for (var o : l2)
		{
			System.out.println(o);
		}
	}
}