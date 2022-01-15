package org.learn2pro.easydb.storage.tests.systemtest;

import org.junit.Before;
import org.learn2pro.easydb.storage.Database;

/**
 * Base class for all SimpleDb test classes.
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {
		Database.reset();
	}

}
