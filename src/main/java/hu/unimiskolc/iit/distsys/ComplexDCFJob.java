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

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;

import java.util.ArrayList;
import java.util.HashMap;

public class ComplexDCFJob extends DCFJob implements ConsumptionEvent {
	private static HashMap<VirtualMachine, DeferredEvent> vmMarkers = new HashMap<VirtualMachine, DeferredEvent>();
	private static HashMap<String, ArrayList<ComplexDCFJob>> coupledJobs = new HashMap<String, ArrayList<ComplexDCFJob>>();
	private static int failingVMCounter = 0;
	private static int vmReuseCount = 0;
	public static final long noJobVMMaxLife = 30000;
	private boolean allowBasicOperations = false;
	private ConsumptionEvent myEvent;
	private VirtualMachine myVM;
	private double availabilityLevel = -1;

	/**
	 * Constructs the job object (to be used by job generators)
	 * 
	 * @param id
	 * @param submit
	 * @param queue
	 * @param exec
	 * @param nprocs
	 * @param ppCpu
	 * @param ppMem
	 * @param user
	 * @param group
	 * @param executable
	 * @param preceding
	 * @param delayAfter
	 */
	public ComplexDCFJob(String id, long submit, long queue, long exec, int nprocs, double ppCpu, long ppMem,
			String user, String group, String executable, Job preceding, long delayAfter) {
		super(id, submit, queue, exec, nprocs, ppCpu, ppMem, user, group, executable, preceding, delayAfter);
	}

	public ComplexDCFJob(ComplexDCFJob toCoupleWith) {
		super(toCoupleWith.getId(), toCoupleWith.getSubmittimeSecs(), toCoupleWith.getQueuetimeSecs(),
				toCoupleWith.getExectimeSecs(), toCoupleWith.nprocs, toCoupleWith.perProcCPUTime,
				toCoupleWith.usedMemory, toCoupleWith.user, toCoupleWith.group, toCoupleWith.executable,
				toCoupleWith.preceding, toCoupleWith.thinkTimeAfterPreceeding);
		ArrayList<ComplexDCFJob> list = coupledJobs.get(getId());
		if (list == null) {
			list = new ArrayList<ComplexDCFJob>();
			list.add(toCoupleWith);
			coupledJobs.put(getId(), list);
		}
		list.add(this);
	}

	/**
	 * Runs the job in the specified VM. Sends out a notification if the job is
	 * completed
	 * 
	 * @param vm
	 * @param completionEvent
	 * @return <i>true</i> if the VM was suitable to use for the particular job
	 *         and the job was never assigned to any VM before, <i>false</i>
	 *         otherwise
	 * @throws NetworkException
	 */
	public boolean startNowOnVM(VirtualMachine vm, ConsumptionEvent completionEvent) throws NetworkException {
		if (vm.getState().equals(VirtualMachine.State.RUNNING)) {
			if (myEvent == null) {
				DeferredEvent de = vmMarkers.remove(vm);
				if (de != null) {
					de.cancel();
					vmReuseCount++;
				}
				vm.newComputeTask(getExectimeSecs() * 1000 * nprocs * ExercisesBase.maxProcessingCap,
						ResourceConsumption.unlimitedProcessing, this);
				allowBasicOperations = true;
				started();
				allowBasicOperations = false;
				myEvent = completionEvent;
				myVM = vm;
				return true;
			}
		}
		return false;
	}

	/**
	 * If the internal representation of the job is somehow cancelled we are
	 * showing that with a runtime exception!
	 */
	public void conCancelled(ResourceConsumption problematic) {
		if (availabilityLevel < 0) {
			throw new RuntimeException("CANCELLED: " + this);
		}
	}

	/**
	 * Marks the completion of the internal representation of the job. Ensures
	 * that calling this function has no effect by anyone else but the
	 * DISSECT-CF simulator. Checks if the VM where the job ran does not have
	 * too long idle time.
	 */
	public void conComplete() {
		if (Thread.currentThread().getStackTrace()[2].getClassName()
				.equals("hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceSpreader$FreqSyncer")) {
			// Only allow the operations take place if the conComplete was not
			// called directly.
			allowBasicOperations = true;
			completed();
			allowBasicOperations = false;
			// Forward the event to the one who started the task on the VM
			myEvent.conComplete();
			DeferredEvent theDE = new DeferredEvent(noJobVMMaxLife) {
				@Override
				protected void eventAction() {
					if (myVM.getState().equals(VirtualMachine.State.RUNNING)) {
						failingVMCounter++;
					}
					vmMarkers.remove(myVM);
				}
			};
			vmMarkers.put(myVM, theDE);
		}
	}

	/**
	 * Masked forwarder for the start events.
	 */
	@Override
	public void started() {
		if (allowBasicOperations) {
			super.started();
		}
	}

	/**
	 * Masked forwarder for the completion events.
	 */
	@Override
	public void completed() {
		if (allowBasicOperations) {
			ArrayList<ComplexDCFJob> list = coupledJobs.remove(getId());
			if (list == null) {
				super.completed();
			} else {
				for (ComplexDCFJob j : list) {
					boolean preAllow = j.allowBasicOperations;
					j.allowBasicOperations = true;
					j.completed();
					j.allowBasicOperations = preAllow;
				}
			}
		}
	}

	/**
	 * Determines the required availability level of the job
	 * 
	 * @return the availability level (if set), otherwise -1
	 */
	public double getAvailabilityLevel() {
		return availabilityLevel;
	}

	/**
	 * Sets the expected level of availability for the job in the range of 0-1.
	 * This function is only callable once! If called repeatedly it throws
	 * IllegalStateExceptions!
	 * 
	 * @param availabilityLevel
	 *            should be in the range of 0-1.
	 * @throws IllegalStateException
	 *             if the function is called more than once!
	 */
	public void setAvailabilityLevel(double availabilityLevel) throws IllegalStateException {
		if (this.availabilityLevel == -1) {
			this.availabilityLevel = availabilityLevel;
		} else {
			throw new IllegalStateException("Cannot alter an already set availability level!");
		}
	}

	/**
	 * Returns the number of VMs that got idle for too much time
	 * 
	 * @return
	 */
	public static int getFailingVMCounter() {
		return failingVMCounter;
	}

	/**
	 * Returns the number of VMs that got reused over the simulation
	 * 
	 * @return
	 */
	public static int getVmReuseCount() {
		return vmReuseCount;
	}

}
