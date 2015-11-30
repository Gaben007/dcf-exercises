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

public class Constants {

	public static final long anHour = 3600000;

	public static final double[] availabilityLevels = { 0.75, 0.9, 0.95, 0.99 };
	public static final double pmAvailability = 0.975;

	// The following values are based on:
	// http://www.slideshare.net/Dell/mtbf-prediction
	public static final long machineLifeTime = 3 * 365 * 24 * anHour; // 3 years
	public static final double machineHourlyFailureRate = 0.025 / 7800;

}
