package com.ery.estorm.http;

import com.ery.estorm.config.Configuration;

public abstract class FilterInitializer {
	/**
	 * Initialize a Filter to a FilterContainer.
	 * 
	 * @param container
	 *            The filter container
	 * @param conf
	 *            Configuration for run-time parameters
	 */
	public abstract void initFilter(FilterContainer container, Configuration conf);

}
