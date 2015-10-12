/*
 *  ========================================================================
 *  dcf-exercises
 *  ========================================================================
 *  
 *  This file is part of dcf-exercises.
 *  
 *  dcf-exercises is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  dcf-exercises is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with dcf-exercises.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2015, Gabor Kecskemeti (kecskemeti@iit.uni-miskolc.hu)
 */
package hu.unimiskolc.iit.distsys;

import java.util.Collection;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class JobtoVMScheduler {
	/**
	 * Constructor to be used with schedulers on VMsets.
	 * 
	 * @param vmset
	 * @param jobs
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public JobtoVMScheduler(Collection<VirtualMachine> vmset, List<Job> jobs)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		final BasicJobScheduler mySched = TestCreatorFactory
				.createARoundRobinScheduler();
		mySched.setupVMset(vmset);
		genericJobDispatcher(mySched, jobs);
	}

	/**
	 * Constructor to be used with schedulers that manage their VM sets on their
	 * own using a single IaaS Service.
	 * 
	 * @param iaas
	 * @param jobs
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public JobtoVMScheduler(IaaSService iaas, List<Job> jobs)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		final BasicJobScheduler mySched = TestCreatorFactory
				.createARoundRobinScheduler();
		mySched.setupIaaS(iaas);
		genericJobDispatcher(mySched, jobs);
	}

	/**
	 * The actual job dispatching operation
	 * 
	 * @param mySched
	 * @param jobs
	 */
	private void genericJobDispatcher(final BasicJobScheduler mySched,
			List<Job> jobs) {
		for (final Job j : jobs) {
			new DeferredEvent(j.getStartTimeInstance() * 1000) {
				@Override
				protected void eventAction() {
					mySched.handleJobRequestArrival(j);
				}
			};
		}
	}
}
