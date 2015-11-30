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

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService.IaaSHandlingException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;

public class PMDeregPreparator implements VirtualMachine.StateChange {
	public interface DeregPreparedCallback {
		void deregistrationPrepared(PMDeregPreparator forPM);
	}

	public final PhysicalMachine pm;
	private final DeregPreparedCallback callback;
	private boolean callbackPending = true;
	private final boolean isGraceful;
	private ArrayList<PhysicalMachine.ResourceAllocation> ras = new ArrayList<PhysicalMachine.ResourceAllocation>();
	private ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();

	public PMDeregPreparator(final PhysicalMachine pm, final DeregPreparedCallback cb, boolean graceful)
			throws SecurityException, IaaSHandlingException, InstantiationException, IllegalAccessException,
			NoSuchFieldException, VMManagementException {
		this.pm = pm;
		callback = cb;
		isGraceful = graceful;
		updateVMSet();
	}

	/**
	 * Analyzes the PM and terminates all VMs that are running on it. It also
	 * kills the PM. It does a deregistration on the PM afterwards.
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
				if (!vm.getState().equals(VirtualMachine.State.RUNNING) || isGraceful) {
					vm.subscribeStateChange(this);
					noSubscription = false;
				} else {
					vm.destroy(true);
				}
			}
		} while (noSubscription && pm.isHostingVMs());
		if (noSubscription) {
			clearAllocations();
			callbackPending = false;
			callback.deregistrationPrepared(this);
		} else {
			addnewAllocation();
		}
	}

	private void addnewAllocation() throws VMManagementException {
		PhysicalMachine.ResourceAllocation ra = pm.allocateResources(pm.freeCapacities, false,
				PhysicalMachine.migrationAllocLen );
		if (ra != null) {
			ras.add(ra);
		}
		new DeferredEvent(PhysicalMachine.migrationAllocLen - 1) {
			@Override
			protected void eventAction() {
				if (callbackPending) {
					clearAllocations();
					try {
						addnewAllocation();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
		};

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
			if (newState.equals(VirtualMachine.State.RUNNING) && !isGraceful) {
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
}
