/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapStub
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MapStub
 * @author Fabian Hueske
 */
public class SampleTask extends AbstractPactTask<Stub> {

	public static final String IN_KEY = "sampling.key.class";

	public static final String IN_VALUE = "sample.value.class";

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<Stub> getStubType() {
		return Stub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception {
		// nothing, since a mapper does not need any preparation
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		// cache references on the stack
		final MutableObjectIterator<PactRecord> input = this.inputs[0];
		final OutputCollector output = this.output;
		
		final PactRecord record = new PactRecord();
		
		int counter = 0;
		while (this.running && input.next(record)) {
			counter++;
			if ((counter % 10) == 0) {
				output.collect(record);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		// SampleTasks need no cleanup, since no strategies are used.
	}
	
}
