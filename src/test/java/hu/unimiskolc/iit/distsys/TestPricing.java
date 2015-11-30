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
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService.IaaSHandlingException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class TestPricing implements MultiCloudUser.CompletionCallback {
	private ArrayList<DeferredEvent> obsolitionEvents = new ArrayList<DeferredEvent>();
	public static final int totalUserCount = 10;
	public static final int initialMachineCount = 30;
	private IaaSService ourService, theCompetition;
	private CostAnalyserandPricer ourAnalyser, competitionAnalyser;
	private int completeUserCount = 0;

	private void prepareIaaS(final IaaSService service) throws Exception {
		service.subscribeToCapacityChanges(new VMManager.CapacityChangeEvent<PhysicalMachine>() {
			@Override
			public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity) {
				final boolean newRegistration = service.isRegisteredHost(affectedCapacity.get(0));
				if (newRegistration) {
					for (final PhysicalMachine pm : affectedCapacity) {
						obsolitionEvents.add(new DeferredEvent(Constants.machineLifeTime) {
							@Override
							protected void eventAction() {
								obsolitionEvents.remove(this);
								// The life of the pm is over
								if (service.isRegisteredHost(pm)) {
									// The pm is not broken so far
									try {
										// Then let's wait for all its VMs and
										// then throw away the PM
										new PMDeregPreparator(pm, new PMDeregPreparator.DeregPreparedCallback() {
											@Override
											public void deregistrationPrepared(PMDeregPreparator forPM) {
												try {
													// The PM has became
													// obsolete
													service.deregisterHost(pm);
												} catch (IaaSHandlingException e) {
													throw new RuntimeException(e);
												}
											}
										}, true);
									} catch (Exception e) {
										throw new RuntimeException(e);
									}
								}
							}
						});
					}
				}
			}
		});

		// Every hour we set the PMs a small likely-hood to fail
		new FaultInjector(Constants.anHour, Constants.machineHourlyFailureRate, service);

		// Here we create the initial cloud infrastructures
		final int maxPMCount = 10000;
		Repository centralStorage = ExercisesBase.getNewRepository(maxPMCount);
		service.registerRepository(centralStorage);

		ArrayList<PhysicalMachine> pmlist = new ArrayList<PhysicalMachine>();
		long minSize = centralStorage.getMaxStorageCapacity();
		for (int i = 0; i < initialMachineCount; i++) {
			PhysicalMachine curr = ExercisesBase.getNewPhysicalMachine();
			pmlist.add(curr);
			minSize = Math.min(minSize, curr.localDisk.getMaxStorageCapacity());
		}
		service.bulkHostRegistration(pmlist);
		VirtualAppliance va = new VirtualAppliance("mainVA", 30, 0, false, minSize / 50);
		centralStorage.registerObject(va);
	}

	@Override
	public void alljobsComplete() {
		completeUserCount++;
		if (completeUserCount == totalUserCount) {
			ourAnalyser.completeCostAnalysis();
			competitionAnalyser.completeCostAnalysis();
			FaultInjector.simulationisComplete = true;
			for (DeferredEvent oe : obsolitionEvents) {
				oe.cancel();
			}
		}
	}

	@Before
	public void preparePricing() throws Exception {
		ourService = ExercisesBase.getNewIaaSService();
		do {
			theCompetition = ExercisesBase.getNewIaaSService();
		} while (theCompetition.pmcontroller.getClass() != ourService.pmcontroller.getClass()
				&& theCompetition.sched.getClass() != ourService.sched.getClass());
		prepareIaaS(ourService);
		ourAnalyser = new CostAnalyserandPricer(ourService);
		CloudProvider ourselves = TestCreatorFactory.getNewProvider();
		ourselves.setIaaSService(ourService);
		prepareIaaS(theCompetition);
		competitionAnalyser = new CostAnalyserandPricer(theCompetition);
		BuiltInCloudProvider competition = new BuiltInCloudProvider();
		competition.setIaaSService(theCompetition);
		int baseDelay = 0;
		for (int i = 0; i < totalUserCount; i++) {
			new DeferredEvent(baseDelay) {
				@Override
				protected void eventAction() {
					try {
						new MultiCloudUser(new IaaSService[] { ourService, theCompetition }, TestPricing.this);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			};
			baseDelay += RandomUtils.nextLong(Constants.anHour, Constants.machineLifeTime / 10);
		}
	}

	@Test(timeout = 60000)
	public void thePricingTest() throws Exception {
		Timed.simulateUntilLastEvent();
		Assert.assertTrue(
				"The final balance of the provider should be positive but was: " + ourAnalyser.getCurrentBalance(),
				ourAnalyser.getCurrentBalance() > 0);
		Assert.assertTrue(
				"The balance of the new provider (" + ourAnalyser.getCurrentBalance()
						+ ") should be greater than the balance of the built in provider ("
						+ competitionAnalyser.getCurrentBalance() + ")",
				ourAnalyser.getCurrentBalance() > competitionAnalyser.getCurrentBalance());
	}
}
