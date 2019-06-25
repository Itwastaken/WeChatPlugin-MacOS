require 'rubygems'
require 'sequel'
require 'json'

def create_db
  db = Sequel.sqlite("wechat-#{ARGV[0]}-sqlite.db")
  db.create_table? :conversation do
    primary_key :id
    String :fromUsrName
    String :toUsrName
    String :createdAt
    String :content
    Blob :image
    Blob :video
    String :messageType

    index :createdAt
    index :fromUsrName
    index :toUsrName
  end

  db
end

def insert_wechat_conversation
  conversation = create_db[:conversation]

  file = File.open(ARGV[0])
  messages = JSON.load file
  file.close
  messages.each do |msg|
    msg2 = {
      content: msg['title'],
      fromUsrName: msg['fromUsrName'],
      toUsrName: msg['toUsrName'],
      createdAt: msg['createdAt'],
      messageType: msg['messageType']
    }

    if msg['messageType'] == 3 || msg['messageType'] == 49 ||
       msg['messageType'] == 47 #image
      if File.exist? msg['url']
        msg2['image'] = Sequel.blob(File.binread(msg['url']))
      end
      msg2.delete(:content) if not msg['url'].start_with? 'http'
    elsif msg['messageType'] == 43 #video
      if File.exist? msg['url']
        msg2['video'] = Sequel.blob(File.binread(msg['url']))
      end
      msg2.delete(:content)
    end
    conversation.insert(msg2)
  end

  puts "conversations: #{conversation.count}"
end

insert_wechat_conversation
