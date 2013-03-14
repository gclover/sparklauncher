
#repositories.remote << 'http://www.ibiblio.org/maven2'
repositories.remote << 'http://mirrors.ibiblio.org/pub/mirrors/maven2'

version = '0.1.0'

core = Layout.new
core[:source, :main, :java] = 'src/main/java'
core[:source, :test, :java] = 'src/test/java'
core[:target, :main, :java] = 'target/main/java'
core[:target, :test, :java] = 'target/test/java'

define 'sparklauncher', :layout => core do
	project.version = version
	compile.with Dir.glob("lib/*.jar")
	classpath = Dir.glob("lib/*.jar")
        package(:jar).with(:manifest => manifest.merge('Class-Path' => classpath.join(' ')))
	package(:jar).include('lib')
	package(:jar)

end




