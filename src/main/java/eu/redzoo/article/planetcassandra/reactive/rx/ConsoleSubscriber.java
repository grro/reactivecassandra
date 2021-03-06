/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.redzoo.article.planetcassandra.reactive.rx;


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

 

public class ConsoleSubscriber implements Subscriber<Object> {
    private Subscription subscription;
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1); // requesting first element starts streaming, implicitly 
    }
    
    @Override
    public void onNext(Object element) {
        System.out.println(element);
        subscription.request(1);  // request next element 
    }
    
    @Override
    public void onError(Throwable t) {
        System.out.println(t.toString());
    }
    
    @Override
    public void onComplete() { }        
}
