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
import java.util.Iterator;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.energy.specialized.IaaSEnergyMeter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.forwarders.PMForwarder;

public class CostAnalyserandPricer extends Timed
		implements VMManager.CapacityChangeEvent<PhysicalMachine>, IaaSForwarder.VMListener {
	private class PMAmortizationRecord {
		public final PhysicalMachine pm;
		private double currentMachinePrice;
		private final double amortizationRate;

		public PMAmortizationRecord(PhysicalMachine forMe) {
			pm = forMe;
			currentMachinePrice = perMachineCost / ((PMForwarder) forMe).getReliMult();
			amortizationRate = currentMachinePrice * hourlyAmortisaiton;
		}

		public void amortizefor(int hours) {
			currentMachinePrice -= hours * amortizationRate;
		}

		public double getCurrentMachinePrice() {
			return currentMachinePrice;
		}
	}

	private class VMUsageRecord implements VirtualMachine.StateChange {
		public final VirtualMachine vm;
		private long startofUsage = -1;
		private long endofUsage = -1;
		public boolean pastPresenceMarker = false;
		public ResourceConstraints rc = null;

		public VMUsageRecord(VirtualMachine forMe) {
			vm = forMe;
			if (!VirtualMachine.preScheduleState.contains(vm.getState())) {
				startofUsage = Timed.getFireCount();
			}
			vm.subscribeStateChange(this);
		}

		@Override
		public void stateChanged(VirtualMachine vm, State oldState, State newState) {
			if (newState.equals(VirtualMachine.State.INITIAL_TR)) {
				if (startofUsage < 0) {
					startofUsage = Timed.getFireCount();
				}
			}
			if (newState.equals(VirtualMachine.State.RUNNING)) {
				rc = new ConstantConstraints(vm.getResourceAllocation().allocated);
			}
			if (newState.equals(VirtualMachine.State.DESTROYED)) {
				endofUsage = Timed.getFireCount();
				vm.unsubscribeStateChange(this);
			}
		}

		public void cancelMonitoring() {
			vm.unsubscribeStateChange(this);
		}

		public boolean isStarted() {
			return startofUsage > 0;
		}

		public boolean isComplete() {
			return endofUsage > 0;
		}

		public long getCompleteUsage() {
			return endofUsage - startofUsage;
		}
	}

	public static final double PUE = 1.6;
	// 45HUF/kwh, the divider is because we calculate in Wms.
	public static final double electricityCost = 45 / 3600000000.0;
	public static final double hourlyAmortisaiton = ((double) Constants.anHour) / Constants.machineLifeTime;
	public static final double perMachineCost = 1000000; // HUF

	private final IaaSService service;
	private final IaaSEnergyMeter meter;
	private final ArrayList<PMAmortizationRecord> pmrecords = new ArrayList<CostAnalyserandPricer.PMAmortizationRecord>();
	private final ArrayList<VMUsageRecord> vmrecords = new ArrayList<VMUsageRecord>();
	private double lastMeterReading = 0;
	private double totalCosts = 0;
	private double totalEarnings = 0;
	private double currentTotal = 0;

	public CostAnalyserandPricer(IaaSService toAnalyse) {
		service = toAnalyse;
		meter = new IaaSEnergyMeter(service);
		meter.startMeter(24 * Constants.anHour, true);
		new DeferredEvent(5 * 60 * 1000) {
			@Override
			protected void eventAction() {
				// TODO Auto-generated method stub
				CostAnalyserandPricer.this.subscribe(24 * Constants.anHour);
			}
		};
		service.subscribeToCapacityChanges(this);
		((IaaSForwarder) service).setVMListener(this);
		for (PhysicalMachine pm : service.machines) {
			pmrecords.add(new PMAmortizationRecord(pm));
		}
	}

	public void completeCostAnalysis() {
		meter.stopMeter();
		unsubscribe();
	}

	@Override
	public void tick(long fires) {
		double currentReading = meter.getTotalConsumption();
		double consumption = currentReading - lastMeterReading; // pure PM
																// related
		lastMeterReading = currentReading;
		consumption *= PUE; // Additional equipment
		consumption *= electricityCost; // electricity price
		totalCosts += consumption;

		double currentAssets = 0;
		for (PMAmortizationRecord pma : pmrecords) {
			pma.amortizefor(24);
			currentAssets += pma.getCurrentMachinePrice();
		}
		Iterator<VMUsageRecord> it = vmrecords.iterator();
		while (it.hasNext()) {
			VMUsageRecord vmu = it.next();
			if (!vmu.isStarted()) {
				// management of the impossible to start VMs
				if (vmu.pastPresenceMarker) {
					vmu.cancelMonitoring();
					it.remove();
				}
				vmu.pastPresenceMarker = true;
			} else if (vmu.isComplete()) {
				totalEarnings += ((IaaSForwarder) service).getResourceQuote(vmu.rc) * vmu.getCompleteUsage();
				it.remove();
			}
		}
		currentTotal = totalEarnings - totalCosts + currentAssets;
	}

	@Override
	public void newVMadded(VirtualMachine[] vms) {
		for (VirtualMachine vm : vms) {
			vmrecords.add(new VMUsageRecord(vm));
		}
	}

	@Override
	public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity) {
		final boolean newRegistration = service.isRegisteredHost(affectedCapacity.get(0));
		if (newRegistration) {
			// Buying resources
			for (PhysicalMachine pm : affectedCapacity) {
				PMAmortizationRecord pma = new PMAmortizationRecord(pm);
				pmrecords.add(pma);
				totalCosts += pma.getCurrentMachinePrice();
			}
		} else {
			// Selling resources
			for (PhysicalMachine pm : affectedCapacity) {
				for (PMAmortizationRecord pma : pmrecords) {
					if (pma.pm == pm) {
						totalEarnings += pma.getCurrentMachinePrice();
						break;
					}
				}
			}
		}
	}

	public double getTotalCosts() {
		return totalCosts;
	}

	public double getTotalEarnings() {
		return totalEarnings;
	}

	public double getCurrentBalance() {
		return currentTotal;
	}
}
