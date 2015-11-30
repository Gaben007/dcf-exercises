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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.ComplexDCFJob;
import hu.unimiskolc.iit.distsys.Constants;
import hu.unimiskolc.iit.distsys.ExercisesBase;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

/**
 * This demonstrator class is showing how to fulfill the high availability
 * requirements of hu.unimiskolc.iit.distsys.TestHighAvailability with the help
 * of replication.
 * 
 * To use this class during the test, set the system property called
 * "hu.unimiskolc.iit.distsys.RRJSched" to
 * "hu.unimiskolc.iit.distsys.solution.SolutionHA". This can be either achieved
 * by setting command line argument of java with the option
 * <code>-Dhu.unimiskolc.iit.distsys.RRJSched=hu.unimiskolc.iit.distsys.solution.SolutionHA</code>
 * , or by setting the system property by calling System.setProperty() before
 * the VMCreatorFactory's getPMFiller() function is called.
 * 
 * Note: for the sake of simplicity and reduced complexity this class is not
 * going to provide 100% success rate on the test.
 * 
 * @author gaborkecskemeti
 * 
 */
public class SolutionHA implements BasicJobScheduler, VirtualMachine.StateChange {
	public static final int[] parallelVMs = { 2, 3, 5, 14 };

	private IaaSService iaas;
	private Repository r;
	private VirtualAppliance va;
	private HashMap<VirtualMachine, Job> vmsWithPurpose = new HashMap<VirtualMachine, Job>();
	private HashMap<String, ArrayList<VirtualMachine>> groupedVMs = new HashMap<String, ArrayList<VirtualMachine>>();

	@Override
	public void handleJobRequestArrival(final Job j) {
		final ComplexDCFJob cdj = (ComplexDCFJob) j;
		cdj.getAvailabilityLevel();
		int vmcount = parallelVMs[Arrays.binarySearch(Constants.availabilityLevels,
				cdj.getAvailabilityLevel())];
		final ConstantConstraints cc = new ConstantConstraints(j.nprocs, ExercisesBase.minProcessingCap,
				ExercisesBase.minMem / j.nprocs);
		for (int i = 0; i < vmcount; i++) {
			// give some spread to our VM requests
			new DeferredEvent(i * 5000) {

				@Override
				protected void eventAction() {
					try {
						// let's run a new VM for our job
						ArrayList<VirtualMachine> vms = groupedVMs.get(j.getId());
						if (vms == null) {
							vms = new ArrayList<VirtualMachine>();
							groupedVMs.put(j.getId(), vms);
						}
						VirtualMachine vm = iaas.requestVM(va, cc, r, 1)[0];
						vm.subscribeStateChange(SolutionHA.this);
						vms.add(vm);
						// let's create a new job instance for the vm
						vmsWithPurpose.put(vm, new ComplexDCFJob(cdj));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			};
		}
	}

	@Override
	public void setupIaaS(IaaSService iaas) {
		this.iaas = iaas;
		r = iaas.repositories.get(0);
		va = (VirtualAppliance) r.contents().iterator().next();
	}

	@Override
	public void setupVMset(Collection<VirtualMachine> vms) {
		// ignore
	}

	@Override
	public void stateChanged(final VirtualMachine vm, State oldState, State newState) {
		final ComplexDCFJob cdj = (ComplexDCFJob) vmsWithPurpose.get(vm);
		if (newState.equals(VirtualMachine.State.RUNNING)) {
			try {
				if (cdj == null) {
					// killing those VMs that got started after some other VM
					// has already performed their job
					vm.destroy(false);
					return;
				}
				cdj.startNowOnVM(vm, new ResourceConsumption.ConsumptionEvent() {
					@Override
					public void conComplete() {
						try {
							ArrayList<VirtualMachine> vms = groupedVMs.get(cdj.getId());
							// we have to make sure no other VMs will continue
							// to run for the particlar job
							for (VirtualMachine currvm : vms) {
								vmsWithPurpose.remove(currvm);
								if (currvm.getState().equals(VirtualMachine.State.RUNNING)) {
									currvm.destroy(true);
								}
							}
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public void conCancelled(ResourceConsumption problematic) {
						if (!vm.getState().equals(VirtualMachine.State.DESTROYED)) {
							try {
								vm.destroy(true);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
					}
				});
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (newState.equals(VirtualMachine.State.DESTROYED) && cdj != null) {
			// If the VM dies prematurely we should forget about the job that
			// was created for it
			vmsWithPurpose.remove(vm);
		}
	}
}
