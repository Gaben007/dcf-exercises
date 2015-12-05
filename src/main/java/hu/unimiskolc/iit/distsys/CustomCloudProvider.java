package hu.unimiskolc.iit.distsys;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class CustomCloudProvider implements CloudProvider, VMManager.CapacityChangeEvent<PhysicalMachine> {
	private IaaSService myProvidedService;
	private CostAnalyserandPricer analyzer;
	
	public static double maxProcessor = 0;
	public static double maxPower = 0;
	public static double maxPowerPerProc = 0;
	public static double allProcessor = 0;
	public static double allRequest = 0;

	@Override
	public double getPerTickQuote(ResourceConstraints rc) {
		maxProcessor = Math.max(rc.getRequiredCPUs(), maxProcessor);
		maxPower = Math.max(rc.getTotalProcessingPower(), maxPower);
		maxPowerPerProc = Math.max(rc.getRequiredProcessingPower(), maxPowerPerProc);
		allProcessor += rc.getRequiredCPUs();
		allRequest++;
		//sumBalance += analyzer.getCurrentBalance();
		//return rc.getTotalProcessingPower() * 0.00000000001;
		//return rc.getTotalProcessingPower() * 0.00000000002;
		//return rc.getRequiredCPUs() * 0.000005;
		
		double effectiveness = myProvidedService.getRunningCapacities().getTotalProcessingPower() / myProvidedService.getCapacities().getTotalProcessingPower();
		double effectivenessBasedPrice = getEffectivenessBasedPerProcPrice(effectiveness);
		double procCountBasedDiscount = getProcCountBasedDiscount(rc.getRequiredCPUs());
		
		return rc.getRequiredCPUs() * effectivenessBasedPrice * procCountBasedDiscount;
	}
	
	private double getEffectivenessBasedPerProcPrice(double effectiveness) {
		if (effectiveness < 0.03)
			return 0;
		
		if (effectiveness < 0.10)
			return 0.000005;
		
		if (effectiveness < 0.30)
			return 0.0005;
		
		if (effectiveness > 0.90)
			return 0.05;
		
		if (effectiveness > 0.80)
			return 0.005;
		
		return 0.000001 * (1 - effectiveness);
	}
	
	private double getProcCountBasedDiscount(double procCount) {
		if (procCount > 30)
			return 0.3;
		
		if (procCount > 10)
			return 0.5;
		
		if (procCount > 5)
			return 0.8;
		
		if (procCount > 3)
			return 0.9;
		
		return 1.0;
	}

	@Override
	public void setIaaSService(IaaSService iaas) {
		myProvidedService = iaas;
		myProvidedService.subscribeToCapacityChanges(this);
		((IaaSForwarder) myProvidedService).setQuoteProvider(this);
	}

	@Override
	public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity) {
		final boolean newRegistration = myProvidedService.isRegisteredHost(affectedCapacity.get(0));
		if (!newRegistration) {
			try {
				for (PhysicalMachine pm : affectedCapacity) {
					// For every lost PM we buy a new one.
					myProvidedService.registerHost(ExercisesBase.getNewPhysicalMachine(RandomUtils.nextDouble(2, 5)));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
