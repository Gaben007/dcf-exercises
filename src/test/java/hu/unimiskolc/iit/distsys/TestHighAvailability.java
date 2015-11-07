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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService.IaaSHandlingException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;


public class TestHighAvailability {
	public static final double[] availabilityLevels = { 0.75, 0.9, 0.95, 0.99 };
	public static final double pmAvailability = 0.975;

	@Before
	public void initVMC() throws Exception
	{
		setDependencies();
		
		Timed.resetTimed();
	}
	
	public void setDependencies() throws Exception {
		Properties p = new Properties();
		p.setProperty("hu.unimiskolc.iit.distsys.RRJSched", "hu.unimiskolc.iit.distsys.HaRRJSched");
		
		System.setProperties(p);
	}
	
	@Test(timeout = 180000)
	public void hatest() throws Exception {
		int[] successCounters = new int[availabilityLevels.length];
		int[] totalCounters = new int[availabilityLevels.length];
		final IaaSService myIaaS = ExercisesBase.getComplexInfrastructure(100);
		Repository r = myIaaS.repositories.get(0);
		VirtualAppliance va = (VirtualAppliance) r.contents().iterator().next();
		AlterableResourceConstraints totCaps = AlterableResourceConstraints.getNoResources();
		double maxNodeProcs = 0;
		for (PhysicalMachine pm : myIaaS.machines) {
			totCaps.singleAdd(pm.getCapacities());
			maxNodeProcs = Math.max(maxNodeProcs, pm.getCapacities().getRequiredCPUs());
		}
		// IaaS is prepared

		// Doing preevaluation of the infrastructure
		VirtualMachine test = myIaaS.requestVM(va, myIaaS.machines.get(0).getCapacities(), r, 1)[0];
		long preTime = Timed.getFireCount();
		Timed.simulateUntilLastEvent();
		long pastTime = Timed.getFireCount();
		long vmCreationTime = pastTime - preTime;
		test.destroy(true);
		Timed.simulateUntilLastEvent();
		Timed.resetTimed();
		// Preevaluation completed

		// Preparing the jobs for the VMs
		RepetitiveRandomTraceGenerator rrtg = new RepetitiveRandomTraceGenerator(ComplexDCFJob.class);
		// total number of jobs
		rrtg.setJobNum(1000);
		// joblist properties
		rrtg.setExecmin(10);
		rrtg.setExecmax(3600);
		rrtg.setMaxgap(0);
		rrtg.setMingap(0);
		rrtg.setMaxStartSpread(3600);
		rrtg.setMaxTotalProcs((int) totCaps.getRequiredCPUs());
		rrtg.setMinNodeProcs(1);
		rrtg.setMaxNodeprocs((int) maxNodeProcs);
		rrtg.setParallel(25);
		final List<Job> jobs = rrtg.getAllJobs();
		final long lastTermination = JobListAnalyser.getLastTerminationTime(jobs) * 1000 * 2;
		for (Job j : jobs) {
			int index = RandomUtils.nextInt(0, availabilityLevels.length);
			((ComplexDCFJob) j).setAvailabilityLevel(availabilityLevels[index]);
			totalCounters[index]++;
		}
		// Joblist is ready

		// Prepares the faulty PMs
		class MyTimed extends Timed {
			ArrayList<VMHandler> myHandlers = new ArrayList<VMHandler>();

			public MyTimed() {
				subscribe(600000);
			}

			class VMHandler implements VirtualMachine.StateChange {
				private final PhysicalMachine pm;
				private ArrayList<PhysicalMachine.ResourceAllocation> ras = new ArrayList<PhysicalMachine.ResourceAllocation>();
				private ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();

				public VMHandler(final PhysicalMachine pm) throws SecurityException, IaaSHandlingException,
						InstantiationException, IllegalAccessException, NoSuchFieldException, VMManagementException {
					myHandlers.add(this);
					this.pm = pm;
					updateVMSet();
				}

				/**
				 * Analyzes the PM and terminates all VMs that are running on
				 * it. It also kills the PM. It does a deregistration on the PM
				 * afterwards.
				 * 
				 * @throws VMManagementException
				 * @throws SecurityException
				 * @throws IaaSHandlingException
				 * @throws InstantiationException
				 * @throws IllegalAccessException
				 * @throws NoSuchFieldException
				 */
				private void updateVMSet() throws VMManagementException, SecurityException, IaaSHandlingException,
						InstantiationException, IllegalAccessException, NoSuchFieldException {
					clearAllocations();
					boolean noSubscription = true;
					do {
						vms.clear();
						vms.addAll(pm.publicVms);
						for (int i = 0; i < vms.size(); i++) {
							VirtualMachine vm = vms.get(i);
							if (!vm.getState().equals(VirtualMachine.State.RUNNING)) {
								vm.subscribeStateChange(this);
								noSubscription = false;
							} else {
								vm.destroy(true);
							}
						}
					} while (noSubscription && pm.isHostingVMs());
					if (noSubscription) {
						doPMReReg(pm);
					} else {
						addnewAllocation();
					}
				}

				private void addnewAllocation() throws VMManagementException {
					PhysicalMachine.ResourceAllocation ra = pm.allocateResources(pm.freeCapacities, false,
							PhysicalMachine.migrationAllocLen * 1000);
					if (ra != null) {
						ras.add(ra);
					}
				}

				/**
				 * Ensures that all VMs that switch to running are terminated
				 * 
				 * @param vm
				 * @param oldState
				 * @param newState
				 */
				@Override
				public void stateChanged(VirtualMachine vm, State oldState, State newState) {
					try {
						if (newState.equals(VirtualMachine.State.RUNNING)) {
							vm.destroy(true);
						} else if (!oldState.equals(VirtualMachine.State.DESTROYED)
								&& newState.equals(VirtualMachine.State.DESTROYED)) {
							addnewAllocation();
							vm.unsubscribeStateChange(this);
							for (VirtualMachine currvm : vms) {
								if (!currvm.getState().equals(VirtualMachine.State.DESTROYED)) {
									return;
								}
							}
							// Ensures that the PM is actually destroyed if
							// there were no new VMs created in the meantime
							updateVMSet();
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				private void clearAllocations() {
					for (PhysicalMachine.ResourceAllocation ra : ras) {
						ra.cancel();
					}
				}

				private void doPMReReg(final PhysicalMachine pm) throws IaaSHandlingException, SecurityException,
						InstantiationException, IllegalAccessException, NoSuchFieldException {
					clearAllocations();
					// drops the old machine
					myIaaS.deregisterHost(pm);

					// adds a new host so we are not short of hosts
					PhysicalMachine newPM = null;
					do {
						if (newPM != null) {
							ExercisesBase.dropPM(newPM);
						}
						newPM = ExercisesBase.getNewPhysicalMachine();
						// let's throw away those replacements that are
						// having too little CPU counts for our purposes
					} while (newPM.getCapacities().getRequiredCPUs() < pm.getCapacities().getRequiredCPUs());
					myIaaS.registerHost(newPM);
					myHandlers.remove(this);
				}
			}

			@Override
			public void tick(long fires) {
				StringBuffer sb = new StringBuffer();
				for (PhysicalMachine pm : myIaaS.machines) {
					sb.append(pm.hashCode() + " ");
				}
				ArrayList<PhysicalMachine> pmlist = new ArrayList<PhysicalMachine>();
				for (PhysicalMachine pm : myIaaS.machines) {
					if (Math.random() >= pmAvailability) {
						pmlist.add(pm);
						for (VMHandler vmh : myHandlers) {
							// ensuring we are not going to delist a PM that is
							// already about to be delisted
							if (vmh.pm == pm) {
								pmlist.remove(pmlist.size() - 1);
							}
						}
					}
				}
				for (PhysicalMachine pm : pmlist) {
					try {
						new VMHandler(pm);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				if (lastTermination < fires) {
					unsubscribe();
				}
			}
		}
		// Faulty PM preparation complete

		// Preparing the scheduling
		new JobtoVMScheduler(myIaaS, jobs);
		new MyTimed();

		Timed.simulateUntilLastEvent();
		
		System.out.println("handle (reused): " + HaRRJSched.handleCallCount);
		System.out.println("retry: " + HaRRJSched.retryRequestCount);
		System.out.println("delayed: " + HaRRJSched.delayedCreationCount);
		System.out.println("unsuccessful: " + HaRRJSched.unsuccessfulCreationCount);
		System.out.println("no resource found: " + HaRRJSched.nullResourceFound);
		
		for (final Job j : jobs) {
			ComplexDCFJob jobconv = (ComplexDCFJob) j;
			if (j.getRealstopTime() >= 0) {
				successCounters[Arrays.binarySearch(availabilityLevels, jobconv.getAvailabilityLevel())]++;
				// More complex tests:
				// Should not allow too slow execution time
				Assert.assertTrue(
						"Every job should run faster or equal than it was originally expected but " + j
								+ " did not do so",
						j.getExectimeSecs() * 3 > j.getRealstopTime() - j.getRealqueueTime());
				// Should not allow too long queueing time
				Assert.assertTrue("Jobs should not queue more than a VM instantiation time but " + j + " did not do so",
						j.getRealqueueTime() < vmCreationTime * 3);
			}
		}

		for (int i = 0; i < availabilityLevels.length; i++) {
			System.out.println(
					availabilityLevels[i] + " " + 
					((double)successCounters[i] / totalCounters[i]) + " " +
					((1 - availabilityLevels[i]) * 0.5) + " " +
					successCounters[i] + " " +
					totalCounters[i]);
		}
		
		for (int i = 0; i < availabilityLevels.length; i++) {
			System.out.println(
					availabilityLevels[i] + " " + 
					((double)successCounters[i] / totalCounters[i]) + " " +
					((1 - availabilityLevels[i]) * 0.5) + " " +
					successCounters[i] + " " +
					totalCounters[i]);
			Assert.assertEquals(
					"Jobs with availability level " + availabilityLevels[i] + " did not get their expected qualities",
					availabilityLevels[i], (double) successCounters[i] / totalCounters[i],
					(1 - availabilityLevels[i]) * 0.5);
		}
	}
}
