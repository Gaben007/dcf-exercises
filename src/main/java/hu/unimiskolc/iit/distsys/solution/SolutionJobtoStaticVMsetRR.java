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
package hu.unimiskolc.iit.distsys.solution;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This demonstrator class is showing how to fulfill the job handling
 * requirements of hu.unimiskolc.iit.distsys.TestRoundRobijJobSched.
 * 
 * To use this class during the test, set the system property called
 * "hu.unimiskolc.iit.distsys.RRJSched" to
 * "hu.unimiskolc.iit.distsys.solution.SolutionJobtoStaticVMsetRR". This can be
 * either achieved by setting command line argument of java with the option
 * <code>-Dhu.unimiskolc.iit.distsys.RRJSched=hu.unimiskolc.iit.distsys.solution.SolutionJobtoStaticVMsetRR</code>
 * , or by setting the system property by calling System.setProperty() before
 * the VMCreatorFactory's createARoundRobinScheduler() function is called.
 * 
 * @author gaborkecskemeti
 * 
 */
public class SolutionJobtoStaticVMsetRR implements BasicJobScheduler {

	private final int defaultFallback = 100;
	// how much time to wait if there were no VMs to send the current job
	private int fallback = defaultFallback;
	// what is the index of the vm in the vmset that we are using next time for
	// job submission
	private int currentId = 0;
	List<VirtualMachine> vmset;

	/**
	 * called when a new job arrives to the system. this job is then tried to be
	 * scheduled for one of the VMs previously specified by the setupVMset
	 * function. if not possible to schedule the jobs immediately, then a
	 * delayed retry policy is applied.
	 */
	public void handleJobRequestArrival(final Job j) {
		final BasicJobScheduler bjs = this;
		int firstId = currentId;
		boolean unscheduled = true;
		do {
			VirtualMachine vm = vmset.get(currentId++);
			currentId = currentId % vmset.size();
			// determining if the VM is actually doing something or not
			if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
				try {
					// sending the task
					vm.newComputeTask(
							// the task will last exactly the amount of secs
							// specified in the job independently from the
							// actual resource requirements of the job
							j.getExectimeSecs()
									* vm.getPerTickProcessingPower() * 1000,
							ResourceConsumption.unlimitedProcessing,
							new ConsumptionEventAdapter() {
								@Override
								public void conComplete() {
									// marking the end of the job so the final
									// checks in TestRoundRobinJobSched will see
									// we completed the job on time
									j.completed();
								}
							});
					// Marking the start time of the job (again for the test
					// case)
					j.started();
					// resetting the wait time to its minimum
					fallback = defaultFallback;
					unscheduled = false;
				} catch (NetworkException ne) {
					throw new RuntimeException("Cannot start new task", ne);
				}
			}
			// determine if we should look for other VMs to host our current job
		} while (firstId != currentId && unscheduled);
		if (unscheduled) {
			// if we were not able to schedule the job right now, then let's
			// wait a little and try again
			new DeferredEvent(fallback) {
				@Override
				protected void eventAction() {
					bjs.handleJobRequestArrival(j);
				}
			};
			fallback *= 1.2;
		}
	}

	public void setupVMset(Collection<VirtualMachine> vms) {
		vmset = new ArrayList<VirtualMachine>(vms);
	}

	public void setupIaaS(IaaSService iaas) {
		// ignore
	}
}
