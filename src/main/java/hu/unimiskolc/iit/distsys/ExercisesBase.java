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

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.AlwaysOnMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.PhysicalMachineController;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.SchedulingDependentMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.FirstFitScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.NonQueueingScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.RandomScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.RoundRobinScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.Scheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.SmallestFirstScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.util.PowerTransitionGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.util.SeedSyncer;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.forwarders.PMForwarder;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;

public class ExercisesBase {
	public static final HashMap<String, Integer> latencyMap = new HashMap<String, Integer>();
	public static final int maxCoreCount = 64;
	public static final double maxProcessingCap = 5000000; // 5Mflop/ms
	public static final long maxMem = 256l * 1024 * 1024 * 1024; // 256 GB
	public static final long minMem = 1024l * 1024 * 1024; // 1 GB;
	public static final long maxDisk = 6l * 1024 * 1024 * 1024 * 1024; // 6 TB
	public static final long minDisk = 60l * 1024 * 1024 * 1024; // 60 GB
	public static final int maxOnDelay = 1000; // s
	public static final int maxOffDelay = 1000;
	public static final double maxIdlePower = 400; // W
	public static final double minIdlePower = 120; // W
	public static final double maxMaxPower = 1200; // W
	public static final double minMaxPower = 300; // W
	public static final double maxMinPower = 40; // W
	public static final double minMinPower = 2; // W
	public static final long minPMInBW = 100l * 1024 * 1024 / 8 / 1000; // 100Mbit
	public static final long maxPMInBW = 10l * 1024 * 1024 * 1024 / 8 / 1000; // 10Gbit
	public static final int minLatency = 100;
	public static final int maxLatency = 100;
	@SuppressWarnings("unchecked")
	public static final Class<? extends Scheduler>[] vmSchClasses = new Class[] { FirstFitScheduler.class,
			NonQueueingScheduler.class, RandomScheduler.class, RoundRobinScheduler.class,
			SmallestFirstScheduler.class };
	@SuppressWarnings("unchecked")
	public static final Class<? extends PhysicalMachineController>[] pmContClasses = new Class[] {
			AlwaysOnMachines.class, SchedulingDependentMachines.class };
	private static int nameID = 0;

	private static ArrayList<PMForwarder> pmfs = new ArrayList<PMForwarder>();
	public static List<PMForwarder> pmforwarders = Collections.unmodifiableList(pmfs);

	private static ArrayList<IaaSForwarder> ifs = new ArrayList<IaaSForwarder>();
	public static List<IaaSForwarder> iaasforwarders = Collections.unmodifiableList(ifs);

	public static String genNewName(String prefix) {
		String newName = prefix + "-" + nameID++;
		latencyMap.put(newName, RandomUtils.nextInt(minLatency, maxLatency));
		return newName;
	}

	public static Repository getNewRepository(final long multiplier) {
		final long networkBW = RandomUtils.nextLong(multiplier * minPMInBW, multiplier * maxPMInBW);
		return new Repository(RandomUtils.nextLong(multiplier * minDisk, multiplier * maxDisk), genNewName("PM"),
				networkBW, networkBW, networkBW / 2, latencyMap);
	}

	public static PhysicalMachine getNewPhysicalMachine()
			throws SecurityException, InstantiationException, IllegalAccessException, NoSuchFieldException {
		double idlePower = RandomUtils.nextDouble(minIdlePower, maxIdlePower);
		double realMinMaxPower = Math.max(idlePower, minMaxPower);
		PMForwarder f = new PMForwarder((double) RandomUtils.nextInt(1, maxCoreCount),
				SeedSyncer.centralRnd.nextDouble() * maxProcessingCap, RandomUtils.nextLong(minMem, maxMem),
				getNewRepository(1), SeedSyncer.centralRnd.nextInt(maxOnDelay),
				SeedSyncer.centralRnd.nextInt(maxOffDelay),
				PowerTransitionGenerator.generateTransitions(RandomUtils.nextDouble(minMinPower, maxMinPower),
						idlePower, RandomUtils.nextDouble(realMinMaxPower, maxMaxPower), 30, 40));
		pmfs.add(f);
		return f;
	}

	public static IaaSService getNewIaaSService() throws IllegalArgumentException, SecurityException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		IaaSForwarder f = new IaaSForwarder(vmSchClasses[RandomUtils.nextInt(0, vmSchClasses.length)],
				pmContClasses[RandomUtils.nextInt(0, pmContClasses.length)]);
		ifs.add(f);
		return f;
	}

	public static IaaSService getComplexInfrastructure(final int pmCount)
			throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException, NoSuchFieldException {
		IaaSService iaas = ExercisesBase.getNewIaaSService();
		Repository centralStorage = ExercisesBase.getNewRepository(pmCount);
		iaas.registerRepository(centralStorage);
		ArrayList<PhysicalMachine> pmlist = new ArrayList<PhysicalMachine>();
		long minSize = centralStorage.getMaxStorageCapacity();
		for (int i = 0; i < pmCount; i++) {
			PhysicalMachine curr = ExercisesBase.getNewPhysicalMachine();
			pmlist.add(curr);
			minSize = Math.min(minSize, curr.localDisk.getMaxStorageCapacity());
		}
		iaas.bulkHostRegistration(pmlist);
		VirtualAppliance va = new VirtualAppliance("mainVA", 30, 0, false, minSize / 50);
		centralStorage.registerObject(va);
		return iaas;
	}

	public static void reset() {
		latencyMap.clear();
		pmfs.clear();
		ifs.clear();
	}

}
