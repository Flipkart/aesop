/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.spring.web;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * The <code>RelayController</code> class is a Spring MVC Controller that displays Relay Metrics. Also provides
 * functionality to edit relay configurations and re-initialize them
 * 
 * @author Regunath B
 * @version 1.0, 12 May 2014 
 */

@Controller
public class RelayController {

    /**
     * Controller for relays page
     */
    @RequestMapping(value = {"/relays","/"}, method = RequestMethod.GET)
    public String relays(ModelMap model, HttpServletRequest request) {
        model.addAttribute("relayInfo","");
        if(request.getServletPath().endsWith(".json")) {
            return "relays-json";
        }
        return "relays";
    }

}
