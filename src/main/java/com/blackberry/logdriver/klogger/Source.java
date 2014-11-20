/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.blackberry.logdriver.klogger;

/**
 *
 * @author dariens
 */
public abstract class Source
{
		private String topic;
		private boolean quickRotate;
		private long quickRotateMessageBlocks;

		public String getTopic()
		{
			return topic;
		}

		public void setTopic(String topic)
		{
			this.topic = topic;
		}

		public boolean getQuickRotate()
		{
			return quickRotate;
		}

		public void setQuickRotate(boolean quickRotate)
		{
			this.quickRotate = quickRotate;
		}

		public long getQuickRotateMessageBlocks()
		{
			return quickRotateMessageBlocks;
		}

		public void setQuickRotateMessageBlocks(long quickRotateMessageBlocks)
		{
			this.quickRotateMessageBlocks = quickRotateMessageBlocks;
		}	
}
