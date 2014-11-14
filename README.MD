# KLogger - A High Performance Producer Client for Kafka

KLogger is a simple service that uses Krackle to receive messages and produce them to Kafka topics. 

**Design Goals**

* Basic: Small code base with few dependencies and external requirements
* Efficient: Consideration placed on minimizing object instantiation to reduce effects of GC
* Configurable: Resource consumption can be configured to tune performance 

**Limitations**

* Security: Klogger is not inteded to be distributed outside a trusted security administration domain--a fancy way of saying that Klogger should be deployed only to hosts intended to have direct access to produce for your Kafka topics.  There is no built-in access control or authentication.

**Author(s)** 

* Will Chartrand (original author)
* [Dave Ariens](<mailto:dariens@blackberry.com>) (current maintainer)

**Building**

Performing a Maven install produces a Debian package that currently installs on Ubuntu based Linux distributions that support upstart services.

**Contributing**

To contribute code to this repository you must be [signed up as an official contributor](http://blackberry.github.com/howToContribute.html).

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.




